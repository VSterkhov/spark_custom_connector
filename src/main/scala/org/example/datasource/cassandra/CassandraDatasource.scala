package org.example.datasource.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.example.datasource.postgres.PostgresWriteBuilder

import java.net.InetSocketAddress
//import java.sql.{DriverManager, ResultSet}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.util

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = CassandraTable.schema


  @throws(classOf[Exception])
  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = new CassandraTable(properties.get("tableName"))
}

class CassandraTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = CassandraTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new CassandraScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)

}

object CassandraTable {
  val schema: StructType = new StructType().add("race_id", LongType)
}

case class ConnectionProperties(host: String,
                                port: Int,
                                dataCenter: String,
                                url: String,
                                user: String,
                                password: String,
                                tableName: String,
                                partitionColumn: String,
                                partitionSize: Int)
/** Read */

class CassandraScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new CassandraScan(ConnectionProperties(
    options.get("host"),
    options.get("port").toInt,
    options.get("dataCenter"),
    options.get("url"),
    options.get("user"),
    options.get("password"),
    options.get("tableName"),
    options.get("partitionColumn"),
    options.get("partitionSize").toInt
  ))
}

object CassandraPartitionsList {
  private def getTableBoundParams(connectionProperties: ConnectionProperties): (Long, Long) = {

    val session = CqlSession.builder
      .addContactEndPoint(new DefaultEndPoint(InetSocketAddress
        .createUnresolved(connectionProperties.host,
          connectionProperties.port)))
      .withLocalDatacenter(connectionProperties.dataCenter).build()


//    val query = s"""
//      SELECT
//        min(${connectionProperties.partitionColumn}) as min,
//        max(${connectionProperties.partitionColumn}) as max
//      FROM ${connectionProperties.tableName}
//    """


    val query = s"SELECT min(race_id) as min, max(race_id) as max FROM cycling.race_winners"

    val rs5 = session.execute(query)

    val res = rs5.one()
    val lowerBound = res.getInt("min")
    val upperBound = res.getInt("max")

    (lowerBound, upperBound)
  }

  def apply(connectionProperties: ConnectionProperties): Array[InputPartition] = {
    val (lowerBound, upperBound) = getTableBoundParams(connectionProperties)
    val partitionSize = connectionProperties.partitionSize
    val partitions = ArrayBuffer[CassandraPartition]()
    for (limit <- lowerBound.to(upperBound).by(partitionSize)) {
      val offset = limit + partitionSize - 1
      partitions +=
        new CassandraPartition(
          connectionProperties.url,
          connectionProperties.user,
          connectionProperties.password,
          connectionProperties.tableName,
          limit,
          offset,
          connectionProperties.host,
          connectionProperties.port,
          connectionProperties.dataCenter
        )
    }
    partitions.toArray
  }
}


class CassandraPartition(val url:String,
                         val user: String,
                         val password: String,
                         val tableName: String,
                         val limit: Long,
                         val offset: Long,
                         val host: String,
                         val port: Int,
                         val dataCenter: String
                        ) extends InputPartition

class CassandraScan(connectionProperties: ConnectionProperties) extends Scan with Batch {
  override def readSchema(): StructType = CassandraTable.schema
  override def toBatch: Batch = this
  override def planInputPartitions(): Array[InputPartition] = CassandraPartitionsList(connectionProperties)
  override def createReaderFactory(): PartitionReaderFactory = new CassandraPartitionReaderFactory(connectionProperties)
}

class CassandraPartitionReaderFactory(connectionProperties: ConnectionProperties)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
    new CassandraPartitionReader(partition.asInstanceOf[CassandraPartition])
}

class CassandraPartitionReader(inputPartition: CassandraPartition) extends PartitionReader[InternalRow] {
  val session = CqlSession.builder
    .addContactEndPoint(new DefaultEndPoint(InetSocketAddress
      .createUnresolved(inputPartition.host,
        inputPartition.port)))
    .withLocalDatacenter(inputPartition.dataCenter).build()

  val resultSet = session.execute("SELECT * FROM cycling.race_winners;")

//  private val connection = DriverManager.getConnection(
//    inputPartition.url, inputPartition.user, inputPartition.password
//  )
//  private val statement = connection.createStatement()
//  private val resultSet = statement.executeQuery(s"""
//      select *
//        from ${inputPartition.tableName}
//        limit ${inputPartition.limit}
//        offset ${inputPartition.offset}
//      """)


//  override def next(): Boolean = resultSet.next()
  override def next(): Boolean = if (resultSet.one() == null) true else false
  override def get(): InternalRow = InternalRow(resultSet.one().getInt(0))
  override def close(): Unit = session.close()
}

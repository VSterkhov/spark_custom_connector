package org.example

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, PreparedStatement, Row}
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{CassandraContainer, PostgreSQLContainer}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.net.InetSocketAddress
//import java.sql.DriverManager
//import java.util.Properties


class CassandraSpec extends AnyFlatSpec with TestContainerForAll {
  override val containerDef = CassandraContainer.Def()

  val testTableName = "users"

  "Cassandra data source" should "read table" in withContainers { container =>
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresReaderJob")
      .getOrCreate()

    spark
      .read
      .format("org.example.datasource.cassandra")
//      .option("url", postgresServer.jdbcUrl)
      .option("user", container.cassandraContainer.getUsername)
      .option("password", container.cassandraContainer.getPassword)
      .option("tableName", testTableName)
      .option("host", container.cassandraContainer.getContainerIpAddress)
      .option("port", container.cassandraContainer.getFirstMappedPort.intValue())
      .option("dataCenter", "datacenter1")
      .option("partitionSize", "10")
      .load()
      .show()
//
//
//    println("="*100)
//    println("Read passed")
//    println("="*100)


    spark.stop()
  }


  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    val session = CqlSession.builder
      .addContactEndPoint(new DefaultEndPoint(InetSocketAddress
        .createUnresolved(container.cassandraContainer.getContainerIpAddress,
          container.cassandraContainer.getFirstMappedPort.intValue())))
      .withLocalDatacenter("datacenter1").build()
    session.execute("CREATE KEYSPACE cycling WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")

    session.execute("CREATE TABLE cycling.race_winners (race_id int, PRIMARY KEY (race_id));")

    session.execute("INSERT INTO cycling.race_winners(race_id) VALUES (65263) IF NOT EXISTS;")
    session.execute("INSERT INTO cycling.race_winners(race_id) VALUES (65264) IF NOT EXISTS;")
    session.execute("INSERT INTO cycling.race_winners(race_id) VALUES (65265) IF NOT EXISTS;")
    session.execute("INSERT INTO cycling.race_winners(race_id) VALUES (65266) IF NOT EXISTS;")
    session.execute("INSERT INTO cycling.race_winners(race_id) VALUES (65267) IF NOT EXISTS;")
    session.execute("INSERT INTO cycling.race_winners(race_id) VALUES (65268) IF NOT EXISTS;")
    session.execute("INSERT INTO cycling.race_winners(race_id) VALUES (65269) IF NOT EXISTS;")

    val rs4 = session.execute("SELECT count(race_id) as count FROM cycling.race_winners;")


    val count = rs4.one().getLong("count")
    println(s"Count is $count")

    val query = s"SELECT min(race_id) as min, max(race_id) as max FROM cycling.race_winners"

    val rs5 = session.execute(query)

    val res: Row = rs5.one()
    val min = res.getInt("min")
    val max = res.getInt("max")

    println(s"Count is $min")
    println(s"Count is $max")


    val rs6 = session.execute("SELECT * FROM cycling.race_winners;")

    println(s"Element is ${rs6.one().getInt(0)}")
    println(s"Element is ${rs6.one().getInt(0)}")

    session.close()

  }


  object Queries {
    lazy val createTableQuery = s"CREATE TABLE $testTableName (user_id BIGINT PRIMARY KEY);"

    lazy val testValues: String = (1 to 50).map(i => s"($i)").mkString(", ")

    lazy val insertDataQuery: String = s"INSERT INTO $testTableName VALUES $testValues;"
  }



}

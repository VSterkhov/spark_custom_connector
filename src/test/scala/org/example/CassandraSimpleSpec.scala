package org.example


import com.datastax.driver.core.{Cluster, KeyspaceMetadata}
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.{CassandraContainer, ContainerDef, PostgreSQLContainer}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.net.InetSocketAddress
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint
import com.dimafeng.testcontainers.{CassandraContainer, ForAllTestContainer}
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.DriverManager
import java.util.Properties
import java.util.stream.Collectors
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import com.datastax.driver.core.{Cluster, ConsistencyLevel, PreparedStatement, Session}


class CassandraSimpleSpec extends AnyFlatSpec with TestContainerForAll {
  override val containerDef = CassandraContainer.Def()

  val testTableName = "users"


  "Cassandra data source" should "read table" in withContainers { container =>
    val session = CqlSession.builder
      .addContactEndPoint(new DefaultEndPoint(InetSocketAddress
        .createUnresolved(container.cassandraContainer.getContainerIpAddress,
          container.cassandraContainer.getFirstMappedPort.intValue())))
      .withLocalDatacenter("datacenter1").build()
    val rs = session.execute("select release_version from system.local")
    val row = rs.one

    row.getString("release_version").length shouldBe 6
  }


  "Cassandra data source" should "create and read table" in withContainers { container =>
    val cluster = Cluster
      .builder
      .withClusterName("datacenter1")
      .addContactPoint(container.cassandraContainer.getContainerIpAddress)
      .withPort(container.cassandraContainer.getFirstMappedPort.intValue())
      .build

    cluster.wait(5000)

    try {
      val sessionClient = cluster.connect("test")


      val keyspaces = sessionClient.getCluster.getMetadata.getKeyspaces.asScala


      val filteredKeyspaces =
        keyspaces
          .filter(km => km.getName.equals("test"))

      assertEquals(1, filteredKeyspaces.length)
    }
  }

  "Cassandra cluster" should "init" in withContainers { container =>

    val ipAddress = container.cassandraContainer.getContainerIpAddress
    val port = container.cassandraContainer.getFirstMappedPort.intValue()

    val clusterInited = Cluster
      .builder
      .addContactPoint(ipAddress)
      .withPort(port)
      .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
      .build


//    val session = clusterInited.connect()
    val session = container.cassandraContainer.getCluster.connect()


  }


    //
//  val session = CqlSession.builder
//    .addContactEndPoint(new DefaultEndPoint(InetSocketAddress
//      .createUnresolved(container.cassandraContainer.getContainerIpAddress,
//        container.cassandraContainer.getFirstMappedPort.intValue())))
//    .withLocalDatacenter("datacenter1").build()



//  val rs = session.execute("select release_version from system.local")
//  val row = rs.one
//
//  row.getString("release_version").length should be > 0


}

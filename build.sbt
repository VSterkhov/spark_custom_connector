name := "spark_custom_connector"

version := "0.1"

scalaVersion := "2.12.13"

val testcontainersScalaVersion = "0.38.8"

val CassandraDriver = "2.1.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.postgresql" % "postgresql" % "42.2.18",

  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-cassandra" % testcontainersScalaVersion % "test",
  "com.datastax.oss" % "java-driver-core" % "4.0.1",
  "com.datastax.cassandra" % "cassandra-driver-core"      % CassandraDriver exclude("com.google.guava", "guava")
)

Test / fork := true

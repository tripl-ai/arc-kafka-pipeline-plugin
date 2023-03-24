import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.3.2"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "3.13.1" % "provided"

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion % "provided"

  // kafka
  val kafka = "org.apache.kafka" %% "kafka" % "2.8.1" intransitive()
  val kafkaClients = "org.apache.kafka" % "kafka-clients" % "2.8.1" intransitive()
  val sparkSQLKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

  // Project
  val etlDeps = Seq(
    scalaTest,

    arc,

    sparkSql,
    sparkHive,
    sparkAvro,

    kafka,
    kafkaClients,
    sparkSQLKafka
  )
}
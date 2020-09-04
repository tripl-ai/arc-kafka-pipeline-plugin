import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.0.0"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "3.2.0" % "provided"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  val sparkAvro = "org.apache.spark" %% "spark-avro" % sparkVersion % "provided"

  // kafka
  val kafka = "org.apache.kafka" %% "kafka" % "2.5.0" intransitive()
  val kafkaClients = "org.apache.kafka" % "kafka-clients" % "2.5.0" intransitive()
  val sparkSQLKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

  // Project
  val etlDeps = Seq(
    scalaTest,

    arc,
    typesafeConfig,

    sparkSql,
    sparkHive,
    sparkAvro,

    kafka,
    kafkaClients,
    sparkSQLKafka
  )
}
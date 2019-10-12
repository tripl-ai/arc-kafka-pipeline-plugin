package ai.tripl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.util._
import ai.tripl.arc.util.TestUtils

class KafkaExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"
  val bootstrapServers = "kafka:29092"
  val timeout = 2000L
  val checkPointPath = "/tmp/checkpoint"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", checkPointPath)
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._

    FileUtils.deleteQuietly(new java.io.File(checkPointPath))
  }

  after {
    session.stop()
    FileUtils.deleteQuietly(new java.io.File(checkPointPath))
  }

  test("KafkaExtract: end-to-end") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 9478)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        name="df",
        description=None,
        inputView=inputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None,
        batchSize=10000,
        retries=0,
        params=Map.empty
      )
    )

    val conf = s"""{
      "stages": [
        {
          "type": "KafkaExtract",
          "name": "try to parse",
          "environments": [
            "production",
            "test"
          ],
          "outputView": "test",
          "bootstrapServers": "${bootstrapServers}",
          "topic": "${topic}",
          "groupID": "${groupId}",
          "timeout": ${timeout},
          "maxPollRecords": 100
        }
      ]
    }"""


    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)
        assert(false)
      }
      case Right((pipeline, _)) => {
        ARC.run(pipeline)
      }
    }
  }

  test("KafkaExtract: Binary") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        name="df",
        description=None,
        inputView=inputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None,
        batchSize=16384,
        retries=0,
        params=Map.empty
      )
    )

    val extractDataset = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        name="df",
        description=None,
        outputView=outputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    val expected = dataset
    val actual = extractDataset.select(col("value"))

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }

  test("KafkaExtract: autoCommit = false") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        name="df",
        description=None,
        inputView=inputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None,
        batchSize=16384,
        retries=0,
        params=Map.empty
      )
    )

    val extractDataset0 = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        name="df",
        description=None,
        outputView=outputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    val extractDataset1 = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        name="df",
        description=None,
        outputView=outputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    assert(spark.catalog.isCached(outputView) === true)

    val expected = extractDataset0.select($"value").as[String]
    val actual = extractDataset1.select($"value").as[String]

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)
    }
    assert(actual.except(expected).count === 0)
    assert(expected.except(actual).count === 0)
  }

  test("KafkaExtract: autoCommit = true") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        name="df",
        description=None,
        inputView=inputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None,
        batchSize=16384,
        retries=0,
        params=Map.empty
      )
    )

    val extractDataset0 = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        name="df",
        description=None,
        outputView=outputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000,
        timeout=timeout,
        autoCommit=true,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    val extractDataset1 = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        name="df",
        description=None,
        outputView=outputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000,
        timeout=timeout,
        autoCommit=true,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    assert(spark.catalog.isCached(outputView) === true)

    val expected = extractDataset0.select($"value").as[String]
    val actual = extractDataset1.select($"value").as[String]

    assert(expected.count === dataset.count)
    assert(actual.count === 0)
  }

  test("KafkaExtract: Structured Streaming") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=true)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    val extractDataset = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        name="df",
        description=None,
        outputView=outputView,
        topic=topic,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        maxPollRecords=10000,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        params=Map.empty
      )
    ).get

    val writeStream0 = extractDataset
      .writeStream
      .queryName("extract")
      .format("memory")
      .start

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    val writeStream1 = readStream
      .selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .start()

    val df = spark.table("extract")

    try {
      // enough time for both producer and consumer to begin
      Thread.sleep(5000)
      assert(df.count > 0)
    } finally {
      writeStream0.stop
      writeStream1.stop
    }
  }
}

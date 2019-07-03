package ai.tripl.arc

import java.net.URI
import java.util.UUID
import java.util.Properties

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.TestUtils

class KafkaLoadSuite extends FunSuite with BeforeAndAfter {

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
    FileUtils.deleteQuietly(new java.io.File(checkPointPath)) 
  }

  after {
    session.stop()
    FileUtils.deleteQuietly(new java.io.File(checkPointPath)) 
  }

  test("KafkaLoad: (value)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

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
    val actual = extractDataset.select($"value")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)
    // ensure partitions are utilised
    // note the default number of partitions is set in the KAFKA_NUM_PARTITIONS environment variable in the docker-compose file
    assert(extractDataset.agg(countDistinct("partition")).take(1)(0).getLong(0) === 10)    
  } 

  test("KafkaLoad: (key, value)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("key", $"id".cast("string"))
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .withColumn("value", to_json(struct($"uniform", $"normal")))
      .select(col("key").cast(BinaryType), col("value").cast(BinaryType))
      .repartition(10)

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
    val actual = extractDataset.select($"key", $"value")

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(false)
      println("expected")
      expected.show(false)  
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)

    // ensure partitions are utilised
    // note the default number of partitions is set in the KAFKA_NUM_PARTITIONS environment variable in the docker-compose file
    assert(extractDataset.agg(countDistinct("partition")).take(1)(0).getLong(0) === 10)    
  }     

  test("KafkaLoad: Structured Streaming") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit var arcContext = TestUtils.getARCContext(isStreaming=true)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    val readStream = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load

    readStream.createOrReplaceTempView(inputView)

    val output = spark.sql(s"""
    SELECT 
      CAST(timestamp AS STRING) AS key
      ,CAST(value AS STRING) as value
    FROM ${inputView}
    """)

    output.createOrReplaceTempView(inputView)

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

    Thread.sleep(2000)
    spark.streams.active.foreach(streamingQuery => streamingQuery.stop)

    // use batch mode to check whether data was loaded
    arcContext = TestUtils.getARCContext(isStreaming=false)
    val actual = extract.KafkaExtractStage.execute(
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

    assert(actual.count > 0)
  }    
}

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

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.admin.AdminClient

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

class KafkaLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"
  val bootstrapServers = "kafka:29092"
  val timeout = 2000L
  val checkPointPath = "/tmp/checkpoint"
  val numPartitions = 10 // see docker-compose KAFKA_NUM_PARTITIONS=10
  val numRows = 100

  val props = new Properties
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .config("spark.sql.streaming.checkpointLocation", checkPointPath)
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    FileUtils.deleteQuietly(new java.io.File(checkPointPath))
  }

  after {
    session.stop()
    FileUtils.deleteQuietly(new java.io.File(checkPointPath))
  }

  test("KafkaLoad: (value) [Binary] 1:1 partition") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString
    val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)

    val dataset = spark.sqlContext.range(0, numRows)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(numPartitions, col("id"))
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        id=None,
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

    // ensure all {numRecords} records are on destination
    val (beginningOffsets, endOffsets) = try {
      val partitionInfos = kafkaDriverConsumer.partitionsFor(topic)
      val topicPartitions = partitionInfos.asScala.map { partitionInfo =>
        new TopicPartition(topic, partitionInfo.partition)
      }.asJava
      (kafkaDriverConsumer.beginningOffsets(topicPartitions).asScala, kafkaDriverConsumer.endOffsets(topicPartitions).asScala)
    } finally {
      kafkaDriverConsumer.close
    }
    assert(
      beginningOffsets
        // join with endOffset by key
        .map { case (k, v) => (k, v, endOffsets.get(k).get) }
        .map { case (k, v0, v1) => (k, v1 - v0) }
        // reduce to count records
        .foldLeft(0L) { case (accumulator, (_, partitionLength)) => accumulator + partitionLength }
      ==
      numRows
    )

    val extractDataset = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        id=None,
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

    val expected = dataset.withColumn("partition", spark_partition_id)
    val actual = extractDataset.select(col("value"), col("partition"))

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
  }

  test("KafkaLoad: (value) [Binary] n:1 partition") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString
    val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
    val sparkNumPartitions = 42

    val dataset = spark.sqlContext.range(0, numRows)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(sparkNumPartitions, col("id"))
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        id=None,
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

    // ensure all {numRecords} records are on destination
    val (beginningOffsets, endOffsets) = try {
      val partitionInfos = kafkaDriverConsumer.partitionsFor(topic)
      val topicPartitions = partitionInfos.asScala.map { partitionInfo =>
        new TopicPartition(topic, partitionInfo.partition)
      }.asJava
      (kafkaDriverConsumer.beginningOffsets(topicPartitions).asScala, kafkaDriverConsumer.endOffsets(topicPartitions).asScala)
    } finally {
      kafkaDriverConsumer.close
    }
    assert(
      beginningOffsets
        // join with endOffset by key
        .map { case (k, v) => (k, v, endOffsets.get(k).get) }
        .map { case (k, v0, v1) => (k, v1 - v0) }
        // reduce to count records
        .foldLeft(0L) { case (accumulator, (_, partitionLength)) => accumulator + partitionLength }
      ==
      numRows
    )

    val extractDataset = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        id=None,
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

    val expected = dataset.select(col("value"), expr(s"spark_partition_id() % ${numPartitions}").as("partition"))
    val actual = extractDataset.select(col("value"), col("partition"))

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
  }

  test("KafkaLoad: (value) [String]") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString
    val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)

    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(numPartitions, col("id"))
      .toJSON
      .select(col("value").cast(StringType))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        id=None,
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

    // ensure all {numRecords} records are there
    val (beginningOffsets, endOffsets) = try {
      val partitionInfos = kafkaDriverConsumer.partitionsFor(topic)
      val topicPartitions = partitionInfos.asScala.map { partitionInfo =>
        new TopicPartition(topic, partitionInfo.partition)
      }.asJava
      (kafkaDriverConsumer.beginningOffsets(topicPartitions).asScala, kafkaDriverConsumer.endOffsets(topicPartitions).asScala)
    } finally {
      kafkaDriverConsumer.close
    }
    assert(
      beginningOffsets
        // join with endOffset by key
        .map { case (k, v) => (k, v, endOffsets.get(k).get) }
        .map { case (k, v0, v1) => (k, v1 - v0) }
        // reduce to count records
        .foldLeft(0L) { case (accumulator, (_, partitionLength)) => accumulator + partitionLength }
      ==
      numRows
    )

    val extractDataset = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        id=None,
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
    val actual = extractDataset.select($"value".cast(StringType))

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
  }

  test("KafkaLoad: (value) [double]") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString
    val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)

    val dataset = spark.sqlContext.range(0, 100)
      .withColumn("uniform", rand(seed=10))
      .select(col("uniform"))

    dataset.createOrReplaceTempView(inputView)

    val thrown0 = intercept[Exception with DetailException] {
      load.KafkaLoadStage.execute(
        load.KafkaLoadStage(
          plugin=new load.KafkaLoad,
          id=None,
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
      ).get
    }

    assert(thrown0.getMessage === "KafkaLoad requires inputView to be dataset with [key: string, value: string], [value: string], [key: binary, value: binary] or [value: binary] signature. inputView 'inputView' has 1 columns of type [double].")
  }

  test("KafkaLoad: (key, value)") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString
    val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)

    val dataset = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("key", $"id".cast("string"))
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .withColumn("value", to_json(struct($"uniform", $"normal")))
      .select(col("key").cast(BinaryType), col("value").cast(BinaryType))
      .repartition(numPartitions, col("key"))

    dataset.createOrReplaceTempView(inputView)

    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        id=None,
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

    // ensure all {numRecords} records are there
    val (beginningOffsets, endOffsets) = try {
      val partitionInfos = kafkaDriverConsumer.partitionsFor(topic)
      val topicPartitions = partitionInfos.asScala.map { partitionInfo =>
        new TopicPartition(topic, partitionInfo.partition)
      }.asJava
      (kafkaDriverConsumer.beginningOffsets(topicPartitions).asScala, kafkaDriverConsumer.endOffsets(topicPartitions).asScala)
    } finally {
      kafkaDriverConsumer.close
    }
    assert(
      beginningOffsets
        // join with endOffset by key
        .map { case (k, v) => (k, v, endOffsets.get(k).get) }
        .map { case (k, v0, v1) => (k, v1 - v0) }
        // reduce to count records
        .foldLeft(0L) { case (accumulator, (_, partitionLength)) => accumulator + partitionLength }
      ==
      numRows
    )

    val extractDataset = extract.KafkaExtractStage.execute(
      extract.KafkaExtractStage(
        plugin=new extract.KafkaExtract,
        id=None,
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
        id=None,
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
        id=None,
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

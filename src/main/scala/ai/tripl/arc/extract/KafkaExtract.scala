package ai.tripl.arc.extract

import java.io._
import java.net.URI
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.MaskUtils

class KafkaExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.kafka.BuildInfo.version

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "KafkaExtract",
    |  "name": "KafkaExtract",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "bootstrapServers": "kafka:9092",
    |  "topic": "topic",
    |  "groupID": "groupID",
    |  "outputView": "outputView"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#kafkaextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "outputView" :: "bootstrapServers" :: "topic" :: "groupID" :: "autoCommit" :: "maxPollRecords" :: "numPartitions" :: "partitionBy" :: "persist" :: "timeout" :: "strict" :: "params" :: "maxRecords" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val outputView = getValue[String]("outputView")
    val topic = getValue[String]("topic")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val groupID = getValue[String]("groupID")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val maxPollRecords = getValue[Int]("maxPollRecords", default = Some(500))
    val timeout = getValue[java.lang.Long]("timeout", default = Some(10000L))
    val autoCommit = getValue[java.lang.Boolean]("autoCommit", default = Some(false))
    val strict = getValue[java.lang.Boolean]("strict", default = Some(true))
    val maxRecords = getOptionalValue[Int]("maxRecords")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, maxRecords, strict, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(outputView), Right(topic), Right(bootstrapServers), Right(groupID), Right(persist), Right(numPartitions), Right(maxPollRecords), Right(timeout), Right(autoCommit), Right(partitionBy), Right(maxRecords), Right(strict), Right(invalidKeys)) =>

        val stage = KafkaExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          outputView=outputView,
          topic=topic,
          bootstrapServers=bootstrapServers,
          groupID=groupID,
          maxPollRecords=maxPollRecords,
          timeout=timeout,
          autoCommit=autoCommit,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          maxRecords=maxRecords,
          strict=strict,
        )

        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("bootstrapServers", bootstrapServers)
        stage.stageDetail.put("groupID", groupID)
        stage.stageDetail.put("topic", topic)
        stage.stageDetail.put("maxPollRecords", java.lang.Integer.valueOf(maxPollRecords))
        maxRecords.foreach { maxRecords => stage.stageDetail.put("maxRecords", java.lang.Integer.valueOf(maxRecords)) }
        stage.stageDetail.put("timeout", java.lang.Long.valueOf(timeout))
        stage.stageDetail.put("autoCommit", java.lang.Boolean.valueOf(autoCommit))
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("strict", java.lang.Boolean.valueOf(strict))
        stage.stageDetail.put("params", MaskUtils.maskParams(params.keySet.filter { key => key.contains("password") }.toList)(params).asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, maxRecords, strict, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class KafkaExtractStage(
    plugin: KafkaExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    outputView: String,
    topic: String,
    bootstrapServers: String,
    groupID: String,
    maxPollRecords: Int,
    maxRecords: Option[Int],
    timeout: Long,
    autoCommit: Boolean,
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String],
    strict: Boolean,
  ) extends ExtractPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    KafkaExtractStage.execute(this)
  }
}

case class KafkaPartition (
  topicPartition: TopicPartition,
  position: Long,
  endOffset: Long
)

object KafkaExtractStage {

  val KAFKA_EXTRACT_OFFSET_KEY = "kafkaExtractOffset"

  case class KafkaRecord (
    key: Array[Byte],
    value: Array[Byte],
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Long,
    timestampType: Int,
  )

  def execute(stage: KafkaExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    // initialise statistics accumulators
    val recordAccumulator = spark.sparkContext.longAccumulator
    val bytesAccumulator = spark.sparkContext.longAccumulator
    val kafkaPartitionAccumulator = spark.sparkContext.collectionAccumulator[KafkaPartition]

    val df = if (arcContext.isStreaming) {
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", stage.bootstrapServers)
        .option("subscribe", stage.topic)
        .load()
    } else {
      // KafkaConsumer properties
      // https://kafka.apache.org/documentation/#consumerconfigs
      val commonProps = new Properties
      commonProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, stage.bootstrapServers)
      commonProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      commonProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      commonProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      commonProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      commonProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, stage.timeout.toString)
      commonProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Math.min(10000, stage.timeout-1).toString)
      commonProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Math.min(500, stage.timeout-1).toString)
      commonProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, Math.min(3000, stage.timeout-2).toString)
      stage.params.foreach { case (key, value) => commonProps.put(key, value) }

      val props = new Properties
      props.putAll(commonProps)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, stage.groupID)

      // first get the number of partitions, their start and end offsets via the driver process so it can be used for mapPartition
      val endOffsets = try {
        val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
        if (!kafkaDriverConsumer.listTopics().asScala.contains(stage.topic)) {
          throw new Exception(s"topic '${stage.topic}' not found in Kafka cluster with bootstrapServers '${stage.bootstrapServers}'.")
        }
        try {
          val partitionInfos = kafkaDriverConsumer.partitionsFor(stage.topic)
          val topicPartitions = partitionInfos.asScala.map { partitionInfo =>
            new TopicPartition(stage.topic, partitionInfo.partition)
          }.asJava
          // cannot retrieve group positions here as 'You can only check the position for partitions assigned to this consumer.'
          kafkaDriverConsumer.endOffsets(topicPartitions)
        } finally {
          kafkaDriverConsumer.close
        }
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stage.stageDetail
        }
      }

      // calculate the maximum number of records for each partition
      val partitionLimits = stage.maxRecords.map { maxRecords => splitN(1 to maxRecords, endOffsets.size).map(_.length.toLong) }

      val limitedEndOffsets = endOffsets

      val stageMaxPollRecords = stage.maxPollRecords
      val stageGroupID = stage.groupID
      val stageTopic = stage.topic
      val stageTimeout = stage.timeout
      val stageAutoCommit = stage.autoCommit

      val df = try {
        spark.sparkContext.parallelize(Seq.empty[String]).repartition(endOffsets.size).mapPartitions {
          partition => {

            def getKafkaRecord(kafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]], endOffset: Long): List[KafkaRecord] = {
              kafkaConsumer.poll(java.time.Duration.ofMillis(stageTimeout)).records(stageTopic).asScala.filter(consumerRecord => {
                // only consume records up to the known endOffset
                consumerRecord.offset < endOffset
              }).map(consumerRecord => {
                // add metrics for tracing
                recordAccumulator.add(1)
                bytesAccumulator.add((if (consumerRecord.key != null) consumerRecord.key.length else 0) + (if (consumerRecord.value != null) consumerRecord.value.length else 0))
                KafkaRecord(consumerRecord.key, consumerRecord.value, consumerRecord.topic, consumerRecord.partition, consumerRecord.offset, consumerRecord.timestamp, consumerRecord.timestampType.id)
              }).toList
            }

            @tailrec
            def getAllKafkaRecords(kafkaConsumer: KafkaConsumer[Array[Byte], Array[Byte]], endOffset: Long, kafkaRecords: List[KafkaRecord], kafkaRecordsAccumulator: List[KafkaRecord]): List[KafkaRecord] = {
              kafkaRecords match {
                case Nil => kafkaRecordsAccumulator
                case _ => {
                  getAllKafkaRecords(kafkaConsumer, endOffset, getKafkaRecord(kafkaConsumer, endOffset), kafkaRecordsAccumulator ::: kafkaRecords)
                }
              }
            }

            // get the partition of this task which maps 1:1 with Kafka partition
            val partitionId = TaskContext.getPartitionId

            val props = new Properties
            props.putAll(commonProps)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, s"${stageGroupID}-${partitionId}")
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, stageMaxPollRecords.toString)

            val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)

            try {
              // try to assign records based on partitionId and extract
              val topicPartition = new TopicPartition(stageTopic, partitionId)
              kafkaConsumer.assign(List(topicPartition).asJava)

              val position = kafkaConsumer.position(topicPartition)

              // apply partition limit
              val endOffset = partitionLimits match {
                case Some(partitionLimits) => Math.min(position + partitionLimits(partitionId), endOffsets.get(topicPartition).longValue)
                case None => endOffsets.get(topicPartition).longValue
              }

              // recursively get batches of records until finished
              val dataset = getAllKafkaRecords(kafkaConsumer, endOffset, getKafkaRecord(kafkaConsumer, endOffset), Nil)

              // send partition offsets to accumulator so it can consumed by driver for
              // - be used to store offsets for KafkaCommitExecute
              // - be compared with final row count
              kafkaPartitionAccumulator.add(KafkaPartition(topicPartition, position, endOffset))

              if (stageAutoCommit) {
                // commit only up to the endOffset retrieved at the start of the job
                val offsets = new java.util.HashMap[TopicPartition, OffsetAndMetadata]()
                offsets.put(topicPartition, new OffsetAndMetadata(endOffset))
                kafkaConsumer.commitSync(offsets)
              }

              dataset.toIterator
            } finally {
              kafkaConsumer.close
            }
          }
        }.toDF
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stage.stageDetail
        }
      }

      df
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions)
          case None => df
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))
    }

    // force persistence if autoCommit=false to prevent double KafkaExtract execution and different offsets
    if ((stage.persist || !stage.autoCommit) && !repartitionedDF.isStreaming) {
      repartitionedDF.persist(arcContext.storageLevel)

      val recordCount = repartitionedDF.count
      stage.stageDetail.put("records", java.lang.Long.valueOf(recordCount))

      val inputMetricsMap = new java.util.HashMap[java.lang.String, java.lang.Long]()
      inputMetricsMap.put("recordsRead", java.lang.Long.valueOf(recordAccumulator.value))
      inputMetricsMap.put("bytesRead", java.lang.Long.valueOf(bytesAccumulator.value))
      stage.stageDetail.put("inputMetrics", inputMetricsMap)

      // store the positions
      val kafkaPartitions = kafkaPartitionAccumulator.value.asScala.toList
      arcContext.userData.put(s"${KafkaExtractStage.KAFKA_EXTRACT_OFFSET_KEY}_${stage.outputView}", kafkaPartitions)

      // log offsets
      val partitions = new java.util.HashMap[Int, java.util.HashMap[String, Long]]()
      kafkaPartitions.foreach { kafkaPartition =>
        val partitionOffsets = new java.util.HashMap[String, Long]()
        partitionOffsets.put("startOffset", kafkaPartition.position)
        partitionOffsets.put("endOffset", kafkaPartition.endOffset)
        partitions.put(kafkaPartition.topicPartition.partition, partitionOffsets)
      }
      stage.stageDetail.put("partitionsOffsets", partitions)

      if (stage.strict) {
        // determine expected rows from partition offsets
        val offsetsSum = kafkaPartitions.foldLeft(0L) { (state, kafkaPartition) =>
          // endOffset equals count not offset of final message
          state + (kafkaPartition.endOffset - kafkaPartition.position)
        }

        if (offsetsSum != recordCount) {
          throw new Exception(s"KafkaExtract should create same number of records in the target ('${stage.outputView}') as exist in source ('${stage.topic}') but source has ${offsetsSum} records and target created ${recordCount} records. This will not work with topics with cleanup.policy='compact'. Set 'strict' to false to ignore.") with DetailException {
            override val detail = stage.stageDetail
          }
        }
      }

    }

    Option(repartitionedDF)
  }

  // splitN a seq into n parts of as close to even length as possible
  def splitN(xs: Seq[Int], n: Int): Seq[Seq[Int]] = {
    val m = xs.length
    val targets = (0 to n).map{ x => math.round((x.toDouble*m)/n).toInt }
    def snip(xs: Seq[Int], ns: Seq[Int], got: Seq[Seq[Int]]): Seq[Seq[Int]] = {
      if (ns.length<2) got
      else {
        val (i,j) = (ns.head, ns.tail.head)
        snip(xs.drop(j-i), ns.tail, got :+ xs.take(j-i))
      }
    }
    snip(xs, targets, Seq.empty)
  }

}


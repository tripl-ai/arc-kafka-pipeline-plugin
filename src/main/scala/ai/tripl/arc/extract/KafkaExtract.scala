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

class KafkaExtract extends PipelineStagePlugin {

  val version = ai.tripl.arc.kafka.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "outputView" :: "bootstrapServers" :: "topic" :: "groupID" :: "autoCommit" :: "maxPollRecords" :: "numPartitions" :: "partitionBy" :: "persist" :: "timeout" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val outputView = getValue[String]("outputView")
    val topic = getValue[String]("topic")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val groupID = getValue[String]("groupID")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val maxPollRecords = getValue[Int]("maxPollRecords", default = Some(10000))
    val timeout = getValue[java.lang.Long]("timeout", default = Some(10000L))
    val autoCommit = getValue[java.lang.Boolean]("autoCommit", default = Some(false))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys) match {
      case (Right(name), Right(description), Right(outputView), Right(topic), Right(bootstrapServers), Right(groupID), Right(persist), Right(numPartitions), Right(maxPollRecords), Right(timeout), Right(autoCommit), Right(partitionBy), Right(invalidKeys)) =>

        val stage = KafkaExtractStage(
          plugin=this,
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
          partitionBy=partitionBy
        )

        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("bootstrapServers", bootstrapServers)
        stage.stageDetail.put("groupID", groupID)
        stage.stageDetail.put("topic", topic)
        stage.stageDetail.put("maxPollRecords", java.lang.Integer.valueOf(maxPollRecords))
        stage.stageDetail.put("timeout", java.lang.Long.valueOf(timeout))
        stage.stageDetail.put("autoCommit", java.lang.Boolean.valueOf(autoCommit))
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, outputView, topic, bootstrapServers, groupID, persist, numPartitions, maxPollRecords, timeout, autoCommit, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class KafkaExtractStage(
    plugin: KafkaExtract,
    name: String,
    description: Option[String],
    outputView: String,
    topic: String,
    bootstrapServers: String,
    groupID: String,
    maxPollRecords: Int,
    timeout: Long,
    autoCommit: Boolean,
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends PipelineStage {

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

  case class KafkaRecord (
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Long,
    key: Array[Byte],
    value: Array[Byte]
  )

  def execute(stage: KafkaExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

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

      val props = new Properties
      props.putAll(commonProps)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, stage.groupID)

      // first get the number of partitions, their start and end offsets via the driver process so it can be used for mapPartition
      val endOffsets = try {
        val kafkaDriverConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
        try {
          val partitionInfos = kafkaDriverConsumer.partitionsFor(stage.topic)
          val topicPartitions = partitionInfos.asScala.map { partitionInfo =>
            new TopicPartition(stage.topic, partitionInfo.partition)
          }.asJava
          kafkaDriverConsumer.endOffsets(topicPartitions)
        } finally {
          kafkaDriverConsumer.close
        }
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          override val detail = stage.stageDetail
        }
      }

      val stageMaxPollRecords = stage.maxPollRecords
      val stageGroupID = stage.groupID
      val stageTopic = stage.topic
      val stageTimeout = stage.timeout
      val stageAutoCommit = stage.autoCommit

      val df = try {
        spark.sqlContext.emptyDataFrame.repartition(endOffsets.size).mapPartitions {
          partition => {
            // get the partition of this task which maps 1:1 with Kafka partition
            val partitionId = TaskContext.getPartitionId

            val props = new Properties
            props.putAll(commonProps)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, s"${stageGroupID}-${partitionId}")

            // try to assign records based on partitionId and extract
            val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](props)
            val topicPartition = new TopicPartition(stageTopic, partitionId)
            val endOffset = endOffsets.get(topicPartition).longValue

            def getKafkaRecord(): List[KafkaRecord] = {
              kafkaConsumer.poll(java.time.Duration.ofMillis(stageTimeout)).records(stageTopic).asScala.filter(consumerRecord => {
                // only consume records up to the known endOffset
                consumerRecord.offset <= endOffset
              }).map(consumerRecord => {
                KafkaRecord(consumerRecord.topic, consumerRecord.partition, consumerRecord.offset, consumerRecord.timestamp, consumerRecord.key, consumerRecord.value)
              }).toList
            }

            @tailrec
            def getAllKafkaRecords(kafkaRecords: List[KafkaRecord], kafkaRecordsAccumulator: List[KafkaRecord]): List[KafkaRecord] = {
                kafkaRecords match {
                    case Nil => kafkaRecordsAccumulator
                    case _ => {
                      getAllKafkaRecords(getKafkaRecord, kafkaRecordsAccumulator ::: kafkaRecords)
                    }
                }
            }

            try {
              // assign only current partition to this task
              kafkaConsumer.assign(List(topicPartition).asJava)

              // find the position and add the difference to the accumulator so it can be compared with row count
              kafkaPartitionAccumulator.add(KafkaPartition(topicPartition, kafkaConsumer.position(topicPartition), endOffset))

              // recursively get batches of records until finished
              val dataset = getAllKafkaRecords(getKafkaRecord, Nil)

              if (stageAutoCommit) {
                // commit only up to the endOffset retrieved at the start of the job
                val offsets = new java.util.HashMap[TopicPartition,OffsetAndMetadata]()
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

      if (!stage.autoCommit) {
        val offsets = new java.util.HashMap[TopicPartition, OffsetAndMetadata]()
        endOffsets.asScala.foreach { case (topicPartition, offset) => {
          offsets.put(topicPartition, new OffsetAndMetadata(offset))
          }
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

      // store the positions
      val kafkaPartitions = kafkaPartitionAccumulator.value.asScala.toList
      arcContext.userData.put("kafkaExtractOffsets", kafkaPartitions)

      // log offsets
      val partitions = new java.util.HashMap[Int, java.util.HashMap[String, Long]]()
      kafkaPartitions.foreach { kafkaPartition =>
        val partitionOffsets = new java.util.HashMap[String, Long]()
        partitionOffsets.put("startOffset", kafkaPartition.position)
        partitionOffsets.put("endOffset", kafkaPartition.endOffset)
        partitions.put(kafkaPartition.topicPartition.partition, partitionOffsets)
      }
      stage.stageDetail.put("partitionsOffsets", partitions)

      // determine expected rows from partition offsets
      val offsetsSum = kafkaPartitions.foldLeft(0L) { (state, kafkaPartition) =>
        state + (kafkaPartition.endOffset - kafkaPartition.position)
      }

      stage.stageDetail.put("records", java.lang.Long.valueOf(recordCount))

      if (offsetsSum != recordCount) {
        throw new Exception(s"KafkaExtract should create same number of records in the target ('${stage.outputView}') as exist in source ('${stage.topic}') but source has ${offsetsSum} records and target created ${recordCount} records.") with DetailException {
          override val detail = stage.stageDetail
        }
      }


    }

    Option(repartitionedDF)
  }

}


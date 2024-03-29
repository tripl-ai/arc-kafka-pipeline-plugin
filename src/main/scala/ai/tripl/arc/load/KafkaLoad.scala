package ai.tripl.arc.load

import java.net.URI
import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.TaskContext

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

import com.typesafe.config._

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
import ai.tripl.arc.util.ListenerUtils
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.MaskUtils
import ai.tripl.arc.extract.KafkaPartition

class KafkaLoad extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.kafka.BuildInfo.version

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "KafkaLoad",
    |  "name": "KafkaLoad",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputView": "inputView",
    |  "bootstrapServers": "kafka:9092",
    |  "topic": "topic"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/load/#kafkaload")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "bootstrapServers" :: "topic" :: "acks" :: "batchSize" :: "numPartitions" :: "retries" :: "params" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val topic = getValue[String]("topic")
    val acks = getValue[Int]("acks", default = Some(-1))
    val retries = getValue[Int]("retries", default = Some(0))
    val batchSize = getValue[Int]("batchSize", default = Some(16384))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, inputView, bootstrapServers, topic, acks, retries, batchSize, numPartitions, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(inputView), Right(bootstrapServers), Right(topic), Right(acks), Right(retries), Right(batchSize), Right(numPartitions), Right(invalidKeys)) =>

      val stage = KafkaLoadStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          topic=topic,
          bootstrapServers=bootstrapServers,
          acks=acks,
          numPartitions=numPartitions,
          retries=retries,
          batchSize=batchSize,
          params=params
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("topic", topic)
        stage.stageDetail.put("bootstrapServers", bootstrapServers)
        stage.stageDetail.put("acks", java.lang.Integer.valueOf(acks))
        stage.stageDetail.put("retries", java.lang.Integer.valueOf(retries))
        stage.stageDetail.put("batchSize", java.lang.Integer.valueOf(batchSize))
        stage.stageDetail.put("params", MaskUtils.maskParams(params.keySet.filter { key => key.contains("password") }.toList)(params).asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, bootstrapServers, topic, acks, retries, batchSize, numPartitions, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }

}

case class KafkaLoadStage(
    plugin: KafkaLoad,
    id: Option[String],
    name: String,
    description: Option[String],
    inputView: String,
    topic: String,
    bootstrapServers: String,
    acks: Int,
    numPartitions: Option[Int],
    retries: Int,
    batchSize: Int,
    params: Map[String, String]
  ) extends LoadPipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    KafkaLoadStage.execute(this)
  }
}

object KafkaLoadStage {

  case class SimpleType(name: String, dataType: DataType)

  def execute(stage: KafkaLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    val signature = "KafkaLoad requires inputView to be dataset with [key: string, value: string], [value: string], [key: binary, value: binary] or [value: binary] signature."

    val kafkaPartitionAccumulator = spark.sparkContext.collectionAccumulator[KafkaPartition]

    val df = spark.table(stage.inputView)

      // enforce schema layout
      val simpleSchema = df.schema.fields.map(field => {
          SimpleType(field.name, field.dataType)
      })
      simpleSchema match {
        case Array(SimpleType("key", StringType), SimpleType("value", StringType)) =>
        case Array(SimpleType("value", StringType)) =>
        case Array(SimpleType("key", BinaryType), SimpleType("value", BinaryType)) =>
        case Array(SimpleType("value", BinaryType)) =>
        case _ => {
          throw new Exception(s"${signature} inputView '${stage.inputView}' has ${df.schema.length} columns of type [${df.schema.map(f => f.dataType.simpleString).mkString(", ")}].") with DetailException {
            override val detail = stage.stageDetail
          }
        }
      }

    val outputDF = if (df.isStreaming) {
      df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", stage.bootstrapServers)
        .option("topic", stage.topic)
        .options(stage.params)
        .start

      df
    } else {

      val repartitionedDF = stage.numPartitions match {
        case Some(partitions) => {
          stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(partitions))
          df.repartition(partitions)
        }
        case None => {
          stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(df.rdd.getNumPartitions))
          df
        }
      }

      // initialise statistics accumulators
      val recordAccumulator = spark.sparkContext.longAccumulator
      val bytesAccumulator = spark.sparkContext.longAccumulator
      val outputMetricsMap = new java.util.HashMap[java.lang.String, java.lang.Long]()

      // the topic so it can be serialised
      val stageTopic = stage.topic

      // create producer on the driver to get numPartitions of target topic
      val props = new Properties
      addCommonProps(stage, props)
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      val kafkaProducer = new KafkaProducer[java.lang.String, java.lang.String](props)
      val topicNumPartitions =  try {
        kafkaProducer.partitionsFor(stageTopic).asScala.length
      } finally {
        kafkaProducer.close
      }

      try {
        repartitionedDF.schema.map(_.dataType) match {
          case List(StringType) => {
            repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
              // get the partition of this task which maps n:1 with Kafka partition
              val partitionId = TaskContext.getPartitionId
              val assignedPartition = java.lang.Integer.valueOf(partitionId % topicNumPartitions)

              val props = new Properties
              addCommonProps(stage, props)
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

              // create producer
              val kafkaProducer = new KafkaProducer[java.lang.String, java.lang.String](props)
              try {
                // send each message via producer
                partition.foreach(row => {
                  // create payload and send sync
                  val value = row.getString(0)

                  val producerRecord = new ProducerRecord[java.lang.String, java.lang.String](stageTopic, assignedPartition, null, value)
                  kafkaProducer.send(producerRecord)
                  recordAccumulator.add(1)
                  bytesAccumulator.add(value.getBytes.length)
                })
              } finally {
                kafkaProducer.close
              }
            }
          }
          case List(BinaryType) => {
            repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
              // get the partition of this task which maps n:1 with Kafka partition
              val partitionId = TaskContext.getPartitionId
              val assignedPartition = java.lang.Integer.valueOf(partitionId % topicNumPartitions)

              val props = new Properties
              addCommonProps(stage, props)
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

              // create producer
              val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](props)
              try {
                // send each message via producer
                partition.foreach{
                  row => {
                    // create payload and send sync
                    val value = row.get(0).asInstanceOf[Array[Byte]]

                    val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](stageTopic, assignedPartition, null, value)
                    kafkaProducer.send(producerRecord)
                    recordAccumulator.add(1)
                    bytesAccumulator.add(value.length)
                  }
                }
              } finally {
                kafkaProducer.close
              }
            }
          }
          case List(StringType, StringType) => {
            repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
              // get the partition of this task which maps n:1 with Kafka partition
              val partitionId = TaskContext.getPartitionId
              val assignedPartition = java.lang.Integer.valueOf(partitionId % topicNumPartitions)

              val props = new Properties
              addCommonProps(stage, props)
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

              // create producer
              val kafkaProducer = new KafkaProducer[String, String](props)
              try {
                // send each message via producer
                partition.foreach(row => {
                  // create payload and send sync
                  val key = row.getString(0)
                  val value = row.getString(1)

                  val producerRecord = new ProducerRecord[String, String](stageTopic, assignedPartition, key, value)
                  kafkaProducer.send(producerRecord)
                  recordAccumulator.add(1)
                  bytesAccumulator.add(key.getBytes.length + value.getBytes.length)
                })
              } finally {
                kafkaProducer.close
              }
            }
          }
          case List(BinaryType, BinaryType) => {
            repartitionedDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
              // get the partition of this task which maps n:1 with Kafka partition
              val partitionId = TaskContext.getPartitionId
              val assignedPartition = java.lang.Integer.valueOf(partitionId % topicNumPartitions)

              val props = new Properties
              addCommonProps(stage, props)
              props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
              props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

              // create producer
              val kafkaProducer = new KafkaProducer[Array[Byte], Array[Byte]](props)
              try {
                // send each message via producer
                partition.foreach(row => {
                  // create payload and send sync
                  val key = row.get(0).asInstanceOf[Array[Byte]]
                  val value = row.get(1).asInstanceOf[Array[Byte]]

                  val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](stageTopic, assignedPartition, key, value)
                  kafkaProducer.send(producerRecord)
                  recordAccumulator.add(1)
                  bytesAccumulator.add(key.length + value.length)
                })
              } finally {
                kafkaProducer.close
              }
            }
          }
        }
      } catch {
        case e: Exception => throw new Exception(e) with DetailException {
          outputMetricsMap.put("recordsWritten", java.lang.Long.valueOf(recordAccumulator.value))
          outputMetricsMap.put("bytesWritten", java.lang.Long.valueOf(bytesAccumulator.value))
          stage.stageDetail.put("outputMetrics", outputMetricsMap)

          override val detail = stage.stageDetail
        }
      }

      outputMetricsMap.put("recordsWritten", java.lang.Long.valueOf(recordAccumulator.value))
      outputMetricsMap.put("bytesWritten", java.lang.Long.valueOf(bytesAccumulator.value))
      stage.stageDetail.put("outputMetrics", outputMetricsMap)

      repartitionedDF
    }

    Option(outputDF)
  }

  // KafkaProducer properties
  // https://kafka.apache.org/documentation/#producerconfigs
  // putAll fails in Scala 2.12 and > jdk 8
  private def addCommonProps(stage: KafkaLoadStage, props: Properties): Unit = {
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, stage.bootstrapServers)
      props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(stage.acks))
      props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(stage.retries))
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(stage.batchSize))
      stage.params.foreach { case (key, value) => props.put(key, value) }
  }

}
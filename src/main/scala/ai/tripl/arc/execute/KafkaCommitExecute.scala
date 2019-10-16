package ai.tripl.arc.execute

import java.util.Properties
import scala.collection.JavaConverters._

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.extract.KafkaPartition

class KafkaCommitExecute extends PipelineStagePlugin {

  val version = ai.tripl.arc.kafka.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "bootstrapServers" :: "groupID" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val bootstrapServers = getValue[String]("bootstrapServers")
    val groupID = getValue[String]("groupID")
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, bootstrapServers, groupID, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(bootstrapServers), Right(groupID), Right(invalidKeys)) =>

        val stage = KafkaCommitExecuteStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          bootstrapServers=bootstrapServers,
          groupID=groupID,
          params=params
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("bootstrapServers", bootstrapServers)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, bootstrapServers, groupID, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class KafkaCommitExecuteStage(
    plugin: KafkaCommitExecute,
    name: String,
    description: Option[String],
    inputView: String,
    bootstrapServers: String,
    groupID: String,
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    KafkaCommitExecuteStage.execute(this)
  }
}

object KafkaCommitExecuteStage {

 def execute(stage: KafkaCommitExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {


    try {
      val kafkaPartitions = arcContext.userData.get("kafkaExtractOffsets") match {
        case Some(kafkaPartitions) => kafkaPartitions.asInstanceOf[List[KafkaPartition]]
        case None => throw new Exception("cannot find previous KafkaExtract commit offsets")
      }

      val props = new Properties
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, stage.bootstrapServers)
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      // loop and commit for each consumer group
      kafkaPartitions.foreach { case (kafkaPartition: KafkaPartition) =>
        val partitionProps = new Properties
        partitionProps.putAll(props)
        partitionProps.put(ConsumerConfig.GROUP_ID_CONFIG, s"${stage.groupID}-${kafkaPartition.topicPartition.partition}")
        val kafkaConsumer = new KafkaConsumer[String, String](partitionProps)

        try {
          // commit only up to the endOffset retrieved at the start of the job
          val offsets = new java.util.HashMap[TopicPartition,OffsetAndMetadata]()
          offsets.put(kafkaPartition.topicPartition, new OffsetAndMetadata(kafkaPartition.endOffset))

          kafkaConsumer.commitSync(offsets)
        } finally {
          kafkaConsumer.close
        }
      }

      // log start and end offsets for each partition
      val partitions = new java.util.HashMap[Int, java.util.HashMap[String, Long]]()
      kafkaPartitions.foreach { kafkaPartition =>
        val partitionOffsets = new java.util.HashMap[String, Long]()
        partitionOffsets.put("startOffset", kafkaPartition.position)
        partitionOffsets.put("endOffset", kafkaPartition.endOffset)
        partitions.put(kafkaPartition.topicPartition.partition, partitionOffsets)
      }
      stage.stageDetail.put("partitionsOffsets", partitions)

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    None
  }
}



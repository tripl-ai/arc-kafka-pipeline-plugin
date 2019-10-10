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
      val commit = arcContext.userData.get("kafkaExtractOffsets").asInstanceOf[java.util.HashMap[TopicPartition, OffsetAndMetadata]]

      val props = new Properties
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, stage.bootstrapServers)
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, stage.groupID)
      val kafkaConsumer = new KafkaConsumer[String, String](props)
      
      try {
        kafkaConsumer.commitSync(commit)
      } finally {
        kafkaConsumer.close
      }

      // log start and end offsets for each partition
      val partitions = new java.util.HashMap[Int, java.util.HashMap[String, Long]]()
      commit.asScala.foreach { case (topicPartition, offsetAndMetadata) => {
          val partitionOffsets = new java.util.HashMap[String, Long]()
          partitionOffsets.put("end", offsetAndMetadata.offset)
          partitions.put(topicPartition.partition, partitionOffsets)
        }
      }
      stage.stageDetail.put("partitionOffsets", partitions)
    
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }
    }

    None
  }
}



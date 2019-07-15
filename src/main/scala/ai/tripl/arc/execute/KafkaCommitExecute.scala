package ai.tripl.arc.execute

import java.util.Properties

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
   
    val df = spark.table(stage.inputView)     

    val offsetsLogMap = new java.util.HashMap[String, Object]()

    try {
      // get the aggregation and limit to 10000 for overflow protection
      val offset = df.groupBy(df("topic"), df("partition")).agg(max(df("offset"))).orderBy(df("topic"), df("partition")).limit(10000)

      val props = new Properties
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, stage.bootstrapServers)
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      offset.collect.foreach(row => {
        val topic = row.getString(0)
        val partitionId = row.getInt(1)
        val offset = row.getLong(2) + 1 // note the +1 which is required to set offset as correct value for next read

        props.put(ConsumerConfig.GROUP_ID_CONFIG, s"${stage.groupID}-${partitionId}")
        val kafkaConsumer = new KafkaConsumer[String, String](props)

        // set the offset
        try {
          val offsetsMap = new java.util.HashMap[TopicPartition,OffsetAndMetadata]()
          offsetsMap.put(new TopicPartition(topic, partitionId), new OffsetAndMetadata(offset))
          kafkaConsumer.commitSync(offsetsMap)

          // update logs
          offsetsLogMap.put(s"${stage.groupID}-${partitionId}", java.lang.Long.valueOf(offset))
          stage.stageDetail.put("offsets", offsetsLogMap) 
        } finally {
          kafkaConsumer.close
        }
      })
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail          
      }
    }

    None
  }
}



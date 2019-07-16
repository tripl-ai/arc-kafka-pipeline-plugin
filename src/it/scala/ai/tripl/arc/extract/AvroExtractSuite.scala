package ai.tripl.arc

import java.net.URI
import org.apache.commons.lang.StringEscapeUtils

import org.apache.http.client.methods.{HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.entity.StringEntity

import scala.io.Source

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util.ConfigUtils._
import ai.tripl.arc.config.ArcPipeline
import com.typesafe.config._
import ai.tripl.arc.util._

class AvroExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _  
  val targetBinaryFile = getClass.getResource("/avro/users.avrobinary").toString 
  val schemaFile = getClass.getResource("/avro/user.avsc").toString
  val outputView = "dataset"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")   

    session = spark
    import spark.implicits._    
  }

  after {
    session.stop()
  }

  test("AvroExtract: Binary with Kafka Schema Registry") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val schema = new Schema.Parser().parse(CloudUtils.getTextBlob(new URI(schemaFile)))
    
    // POST the schema to kafka so we know it is there as ID=1
    val httpClient = HttpClients.createDefault
    val httpPost = new HttpPost("http://kafka-schema-registry:8081/subjects/Kafka-value/versions");
    httpPost.setEntity(new StringEntity(s"""{"schema": "${StringEscapeUtils.escapeJavaScript(schema.toString)}"}"""));
    httpPost.addHeader("Content-Type", "application/vnd.schemaregistry.v1+json") 
    val response = httpClient.execute(httpPost)
    assert(response.getStatusLine.getStatusCode == 200)
    response.close
    httpClient.close

    val conf = s"""{
      "stages": [
        {
          "type": "BytesExtract",
          "name": "get the binary avro file without header",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${targetBinaryFile}",
          "outputView": "bytes_extract_output"
        },
        {
          "type": "AvroExtract",
          "name": "try to parse",
          "description": "load customer avro extract",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "bytes_extract_output",
          "outputView": "avro_extract_output",
          "persist": false,
          "inputField": "value",
          "avroSchemaURI": "http://kafka-schema-registry:8081/schemas/ids/1"
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


}

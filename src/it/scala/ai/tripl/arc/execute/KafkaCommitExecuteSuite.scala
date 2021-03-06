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

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.util._
import ai.tripl.arc.util.TestUtils

class KafkaCommitExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView0 = "inputView0"
  val inputView1 = "inputView1"
  val outputView = "outputView"
  val bootstrapServers = "kafka:29092"
  val timeout = 2000L

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

  test("KafkaCommitExecute") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset0 = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset0.createOrReplaceTempView(inputView0)
    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        id=None,
        name="df",
        description=None,
        inputView=inputView0,
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None,
        batchSize=16384,
        retries=0,
        params=Map.empty
      )
    )

    // read should have no offset saved as using uuid group id so get all 100 records
    val extractDataset0 = extract.KafkaExtractStage.execute(
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
        maxRecords=None,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        strict=true,
        params=Map.empty
      )
    ).get

    var expected = dataset0
    var actual = extractDataset0.select("value")
    assert(actual.count == 100)
    assert(actual.except(expected).count == 0)
    assert(expected.except(actual).count == 0)

    // read should have no offset saved (autoCommit=false) so get all 100 records
    val extractDataset1 = extract.KafkaExtractStage.execute(
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
        maxRecords=None,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        strict=true,
        params=Map.empty
      )
    ).get

    expected = dataset0
    actual = extractDataset1.select("value")
    assert(actual.count == 100)
    assert(actual.except(expected).count == 0)
    assert(expected.except(actual).count == 0)

    // execute the update
    ai.tripl.arc.execute.KafkaCommitExecuteStage.execute(
      ai.tripl.arc.execute.KafkaCommitExecuteStage(
        plugin=new ai.tripl.arc.execute.KafkaCommitExecute,
        id=None,
        name="df",
        description=None,
        inputView=outputView,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        params=Map.empty
      )
    )

    // read should now have offset saved so as no new records exist in kafka should return 0 records
    val extractDataset2 = extract.KafkaExtractStage.execute(
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
        maxRecords=None,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        strict=true,
        params=Map.empty
      )
    ).get
    actual = extractDataset2.select("value")
    assert(actual.count == 0)

    // insert 200 records
    val dataset1 = spark.sqlContext.range(0, 200)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset1.createOrReplaceTempView(inputView1)
    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        id=None,
        name="df",
        description=None,
        inputView=inputView1,
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None,
        batchSize=16384,
        retries=0,
        params=Map.empty
      )
    )

    // read should now have offset saved so should only retieve records from second insert (200 records)
    val extractDataset3 = extract.KafkaExtractStage.execute(
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
        maxRecords=None,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        strict=true,
        params=Map.empty
      )
    ).get

    expected = dataset1
    actual = extractDataset3.select("value")
    assert(actual.count == 200)
    assert(actual.except(expected).count == 0)
    assert(expected.except(actual).count == 0)
  }

  test("KafkaCommitExecute: Limit") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val topic = UUID.randomUUID.toString
    val groupId = UUID.randomUUID.toString

    // insert 100 records
    val dataset0 = spark.sqlContext.range(0, 100)
      .select("id")
      .withColumn("uniform", rand(seed=10))
      .withColumn("normal", randn(seed=27))
      .repartition(10)
      .toJSON
      .select(col("value").cast(BinaryType))

    dataset0.createOrReplaceTempView(inputView0)
    load.KafkaLoadStage.execute(
      load.KafkaLoadStage(
        plugin=new load.KafkaLoad,
        id=None,
        name="df",
        description=None,
        inputView=inputView0,
        topic=topic,
        bootstrapServers=bootstrapServers,
        acks= -1,
        numPartitions=None,
        batchSize=16384,
        retries=0,
        params=Map.empty
      )
    )

    // read should have no offset saved as using uuid group id so get records limited to 17
    val extractDataset0 = extract.KafkaExtractStage.execute(
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
        maxRecords=Some(17),
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        strict=true,
        params=Map.empty
      )
    ).get

    var expected = dataset0
    var actual = extractDataset0.select("value")
    assert(actual.count == 17)

    // execute the update
    ai.tripl.arc.execute.KafkaCommitExecuteStage.execute(
      ai.tripl.arc.execute.KafkaCommitExecuteStage(
        plugin=new ai.tripl.arc.execute.KafkaCommitExecute,
        id=None,
        name="df",
        description=None,
        inputView=outputView,
        bootstrapServers=bootstrapServers,
        groupID=groupId,
        params=Map.empty
      )
    )

    // read should now have offset saved so should return 100-17 records
    val extractDataset1 = extract.KafkaExtractStage.execute(
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
        maxRecords=None,
        timeout=timeout,
        autoCommit=false,
        persist=true,
        numPartitions=None,
        partitionBy=Nil,
        strict=true,
        params=Map.empty
      )
    ).get

    actual = extractDataset1.select("value")
    assert(actual.count == 100-17)
  }
}

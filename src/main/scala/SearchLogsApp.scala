import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object MyApp extends App {
  // TODO collect these from args
  val input = "wasbs://nbcs@chstone.blob.core.windows.net/*.json.gz"
  val output = "wasbs://nbcs-stats@chstone.blob.core.windows.net/stats.json"

  val spark = SparkSession
    .builder
    .appName("Search Logs")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val schema = new StructType()
    .add("time", TimestampType)
    .add("operationName", StringType)
    .add("operationVersion", StringType)
    .add("resultType", StringType)
    .add("resultSignature", IntegerType)
    .add("durationMS", IntegerType)
    .add("properties", new StructType()
      .add("Description", StringType)
      .add("Query", new StructType()
        .add("search", StringType)
        .add("searchMode", StringType)
        .add("$filter", StringType)
        .add("$top", IntegerType)
        .add("$skip", IntegerType)
        .add("$orderby", StringType)
        .add("scoringProfile", StringType))
      .add("Documents", IntegerType)
      .add("IndexName", StringType))

  val df = spark.read
    .schema(schema)
    .json(input)

  val oneHour = window($"time", "1 hour")

  df
    .groupBy(oneHour, $"operationName")
    .agg(
      count("operationName").as("total"),
      avg("durationMS").as("avgLatency"))
    .orderBy("window")
    .write
    .json(output)

  spark.stop
}

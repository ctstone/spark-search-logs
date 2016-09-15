import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, StructField, TimestampType, StringType, IntegerType }

object MyApp extends App {
  val spark = SparkSession.builder()
    .master("spark://chstone2:7077")
    .config("spark.jars", "/usr/hadoop/share/hadoop/tools/lib/hadoop-azure-2.7.3.jar,/usr/hadoop/share/hadoop/tools/lib/azure-storage-2.0.0.jar")
    .appName("Search Logs")
    .getOrCreate()

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
    .json("wasbs://nbcs@chstone.blob.core.windows.net/2016-08-01-*.json.gz")

  df.printSchema
  spark.stop
}

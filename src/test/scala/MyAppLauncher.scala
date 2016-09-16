
object MyAppLauncher extends App {
  val cmdArgs = Array(
    "wasbs://nbcs@chstone.blob.core.windows.net/*.json.gz",
    "wasbs://nbcs-stats@chstone.blob.core.windows.net/stats.json")
  System.setProperty("spark.master", "local[*]")
  MyApp.main(cmdArgs)
}

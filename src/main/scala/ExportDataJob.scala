import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ExportDataJob {

  // Initialize SparkSession
  val spark: SparkSession = SparkSession.builder()
    .appName("Yearly Export Data Job")
    .master("local[*]") // Running locally; adjust as needed for production
    .getOrCreate()

  // Function to load data for a specific year and commodity
  def loadData(filePath: String, year: Int, commodity: String): DataFrame = {
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // Debugging statements
    df.printSchema()
    df.show(5)

    // Ensure required columns exist
    val expectedColumns = Seq("year", "Commodity", "country", "value", "HSCode")
    if (!expectedColumns.forall(df.columns.contains)) {
      throw new RuntimeException("One or more expected columns are missing in the input data.")
    }

    // Normalize the commodity input for comparison
    val normalizedCommodity = commodity.toLowerCase.trim
    val filteredDF = df.filter(
      col("year") === year &&
        array_contains(
          split(lower(trim(col("commodity"))), "[.,\" -]+"),
          normalizedCommodity
        )
    )

    if (filteredDF.count() == 0) {
      throw new RuntimeException(s"No data found for year $year and commodity '$commodity'")
    }

    filteredDF
  }


  // Function to aggregate exports by country
  def aggregateExportsByCountry(df: DataFrame): DataFrame = {
    df.groupBy("country").agg(sum("value").alias("total_export"))
  }

  // Function to categorize countries based on export values
  def categorizeCountries(df: DataFrame): DataFrame = {
    df.withColumn("category", when(col("total_export") > 10, "big")
      .when(col("total_export") > 1, "medium")
      .otherwise("small"))
  }

  // Function to save data partitioned by category
  def savePartitionedData(df: DataFrame, year: Int, commodity: String, outputPath: String): Unit = {
    df.write
      .partitionBy("category")
      .option("header", "true")
      .mode("overwrite")
      .csv(s"$outputPath/$year/$commodity")
  }

  // Main job function
  def runJob(filePath: String, year: Int, commodity: String, outputPath: String): Unit = {
    try {
      val data = loadData(filePath, year, commodity)
      val aggregatedData = aggregateExportsByCountry(data)
      val categorizedData = categorizeCountries(aggregatedData)
      savePartitionedData(categorizedData, year, commodity, outputPath)
    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new IllegalArgumentException("Usage: ExportDataJob <year> <commodity>")
    }

    val year = args(0).toInt
    // Join arguments for the commodity to handle spaces correctly
    val commodity = args.drop(1).mkString(" ")
    val filePath = "/Users/abhishekkumar/Downloads/sparkAssignQues3/src/main/resources/data/2018-2010_export.csv"
    val outputPath = "/Users/abhishekkumar/Downloads/sparkAssignQues3/output"

    runJob(filePath, year, commodity, outputPath)
  }
}

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ExportDataJobTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("ExportDataJobTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("loadData should filter data correctly by year and commodity") {
    // Sample data for testing
    val testData = Seq(
      (2018, "CEREALS.", "India", 100.0, "HS001"),
      (2018, "CEREALS WHEAT", "USA", 150.0, "HS002"),
      (2019, "RICE", "India", 200.0, "HS003")
    ).toDF("year", "Commodity", "country", "value", "HSCode")

    // Write test data to a temporary location
    val tempFilePath = "/Users/abhishekkumar/Downloads/sparkAssignQues3/src/test/resources/data/test_export_data"
    testData.write.mode("overwrite").option("header", "true").csv(tempFilePath)

    // Call the function under test
    val resultDF: DataFrame = ExportDataJob.loadData(tempFilePath, 2018, "CEREALS")

    // Assert that the result contains only the rows matching the criteria
    assert(resultDF.count() == 2)
    assert(resultDF.filter($"country" === "India").count() == 1)
    assert(resultDF.filter($"country" === "USA").count() == 1)
  }

  test("aggregateExportsByCountry should aggregate export values correctly") {
    // Sample data for testing
    val testData = Seq(
      ("India", 100.0),
      ("India", 50.0),
      ("USA", 150.0)
    ).toDF("country", "value")

    val resultDF: DataFrame = ExportDataJob.aggregateExportsByCountry(testData)

    // Assert that the aggregation is correct
    val indiaExport = resultDF.filter($"country" === "India").select("total_export").collect()(0).getDouble(0)
    val usaExport = resultDF.filter($"country" === "USA").select("total_export").collect()(0).getDouble(0)

    assert(indiaExport == 150.0)
    assert(usaExport == 150.0)
  }

  test("categorizeCountries should categorize countries based on total export values") {
    // Sample data for testing
    val testData = Seq(
      ("India", 150.0),
      ("USA", 5.0),
      ("UK", 0.5)
    ).toDF("country", "total_export")

    val resultDF: DataFrame = ExportDataJob.categorizeCountries(testData)

    // Assert the correct categorization
    assert(resultDF.filter($"country" === "India" && $"category" === "big").count() == 1)
    assert(resultDF.filter($"country" === "USA" && $"category" === "medium").count() == 1)
    assert(resultDF.filter($"country" === "UK" && $"category" === "small").count() == 1)
  }
}

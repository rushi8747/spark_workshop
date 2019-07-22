
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object TelliusAssignments {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Tellius Assignments")
    .getOrCreate()

  def getDF(path: String, options: Map[String, String], source: String) = {
    spark.read.options(options).format(source).load(path)
  }

  def getCount(dataFrame: DataFrame) = dataFrame.count()

  def salesByWeek(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("week_of_year", weekofyear(col("date")))
      .groupBy(col("week_of_year"))
      .agg(sum("amount").as("total_sales_per_week"))
      .orderBy("week_of_year")
  }

  def nullHandler(dfWithNull: DataFrame) = {
    val dataTypeList = dfWithNull.dtypes

    dataTypeList.foldLeft(dfWithNull) {
      case (memodf, colNameType) =>
        if (!colNameType._2.contains("String") && !colNameType._2.contains(
              "Timestamp")) {
          memodf.withColumn(
            colNameType._1,
            when(col(colNameType._1).isNull,
                 memodf
                   .select(mean(colNameType._1))
                   .first()(0)
                   .asInstanceOf[Double]).otherwise(col(colNameType._1)))
        } else {
          memodf.withColumn(colNameType._1, col(colNameType._1))
        }
    }
  }

  def joinWithProjection(salesDf: DataFrame,
                         customersDf: DataFrame,
                         productsDf: DataFrame): DataFrame = {
    val joinedSalesWithCustomerProduct = salesDf
      .join(customersDf, salesDf("customerId") === customersDf("customerId"))
      .join(productsDf, salesDf("itemId") === productsDf("itemId"))
      .select(customersDf("name") as ("customerName"),
              salesDf("date"),
              salesDf("customerId"),
              salesDf("itemId"),
              productsDf("category"),
              salesDf("amount"))

    joinedSalesWithCustomerProduct
  }

  def addSequentialRowId(dataFrame: DataFrame): DataFrame = {
    val w = Window.orderBy("customerName")
    val result = dataFrame.withColumn("id", row_number().over(w))
    result
  }

  def salesBydiscount(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("discount_amount", col("amount") * (0.95))
  }

  def main(args: Array[String]): Unit = {
    val options = Map("inferschema" -> "true", "sep" -> ",", "header" -> "true")

    val customersDF = getDF("src/main/resources/customers.csv", options, "csv")
    val productsDF = getDF("src/main/resources/products.csv", options, "csv")
    val salesDF = getDF("src/main/resources/sales.csv", options, "csv")

    val salesCount = getCount(salesDF)
    val customerCount = getCount(customersDF)
    val productsCount = getCount(productsDF)

    val salesNoNullDF = nullHandler(salesDF)

    val joinedSalesWithCustomerProduct =
      joinWithProjection(salesDF, customersDF, productsDF)

    val result = addSequentialRowId(joinedSalesWithCustomerProduct) //joinedSalesWithCustomerProduct.withColumn("id", monotonically_increasing_id())

    val getSalesByweek = salesByWeek(salesDF)
    getSalesByweek.show()

    val getSalesBydiscount = salesBydiscount(result)
    getSalesByweek.show()

  }
}

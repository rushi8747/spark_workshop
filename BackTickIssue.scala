package AdvancedSQL

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, when}

object BackTickIssue {

  val addBackTick = (colName: String) => "`" + colName + "`"

  def filterAnalyticalData(inputDF: DataFrame) = {
    //val audienceInclusionDF = inputDF.where(col("AudienceInclusionFlag").===("Y"))
    val columnNames = inputDF.columns
    val filteredColumns = columnNames.map(addBackTick)

    inputDF.na.drop("all", filteredColumns)
  }

  def getColNameList(colSeq: Seq[String]) = {
    var colNames = Seq[Column]()
    colNames = colSeq.map(name => col(addBackTick(name)))
    colNames
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("SG").getOrCreate()
    import spark.implicits._

    val dataFrameWithNull = spark.read.options(Map("header"->"true")).csv("/home/rushi/IdeaProjects/spark_workshop/src/main/resources/databricks/crfd_1365.csv")
    val columnNames: Array[String] = dataFrameWithNull.columns
    val filteredColumns: Array[String] = columnNames.map(addBackTick)

    val inputDf = dataFrameWithNull.select(getColNameList(columnNames): _*)

    val requiredColumns = Seq("Member_id")

    val nullFillDF=requiredColumns.foldLeft(inputDf)((df, colName) =>
      df.withColumn(colName, when(col(colName).isNull || col(colName) === "\\N", null).otherwise(col(colName))))

    val nullFilteredDF=nullFillDF.na.drop("any",requiredColumns)

    nullFilteredDF.show()

    /*val nullFillDF = filteredColumns.foldLeft(inputDf)((df, colName) =>
      df.withColumn(colName, when(col(colName).isNull || col(colName) === "\\N", null).otherwise(col(colName)))
    ).drop(columnNames:_*)
*/







    /*  val nullFillDF=filteredColumns.foldLeft(dataFrameWithNull)((df, colName) =>
    df.withColumn(colName, when(col(colName).isNull || col(colName) === "\\N", null).otherwise(col(colName)))
   )
   nullFillDF.na.drop("all", columnNames).show()
*/


    }


  }

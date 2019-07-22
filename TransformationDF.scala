package RandomProg


import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TransformationDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("SG").getOrCreate()
    import spark.implicits._

    def withGreeting()(df:DataFrame):DataFrame = {
      df.withColumn("greeting",lit("HEY!!"))
    }

    def withFavActivity()(df:DataFrame):DataFrame = {
      df.withColumn("favorite Activity",lit("Surfing"))
    }

    val df = List(("jao"),("gabrial")).toDF()

    df.transform(withGreeting())
      .transform(withFavActivity())
    //  .show()

    def removeAllWhiteSpaces(col:Column):Column = {
      regexp_replace(col,"\\s+","")
    }

    List(("I LIKE FOOD"),("      this food")).toDF("words")
      .withColumn("clean words",removeAllWhiteSpaces($"words"))
      .show()






  }

}

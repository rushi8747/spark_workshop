package AdvancedSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object DateTime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("SG").getOrCreate()
    import spark.implicits._

    val salesDF = spark.read.option("header","true").option("inferSchema","true")
      .csv("/home/rushi/IdeaProjects/spark_workshop/src/main/resources/Tellius/sales.csv")

    salesDF.withColumn("date",date_format(salesDF("date"),"yyyy-dd-MM"))

    salesDF.withColumn("MOST_REVENUEDAY", date_format(salesDF("date"),"EEEE"))

    salesDF.groupBy(salesDF("customerId"))
        .max(("amount")).alias("MAXAMOUT")

      .show()


  }



}

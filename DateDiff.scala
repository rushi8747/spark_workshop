package AdvancedSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DateDiff {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("SG").getOrCreate()
    import spark.implicits._

    val salesDF = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/home/rushi/IdeaProjects/spark_workshop/src/main/resources/Tellius/sales.csv")

    salesDF.withColumn("DateDiff",date_sub(col("date"),25))

    //salesDF.groupBy($"customerId").count().orderBy(col("count").desc).show()

    val windowSpec = Window.partitionBy(col("customerId")).orderBy(col("amount").desc)
   // salesDF.withColumn("count",count("*").over(windowSpec)).show()

    salesDF.filter(not(col("customerId").isNull) )
      .withColumn("rank",dense_rank() over windowSpec)
      .select(col("*")).where(col("rank") === 1).drop("rank")

    val lstOfColumns: Array[String] = salesDF.columns

    val  meanCOl = mean(salesDF("amount"))

//    salesDF.where(when(col("amount").isNull,meanCOl).otherwise(col("amount"))).show()

    salesDF.withColumn("Encrypted",format_number(col("transactionId"),0))
     // .show()


    val customerDf = spark.read.options(Map("header"->"true","inferSchema"->"true"))
      .csv("/home/rushi/IdeaProjects/spark_workshop/src/main/resources/Tellius/customers.csv")

    customerDf.withColumn("Encrypted",regexp_replace(col("name"),"a|e|i","X"))
      .drop("name").withColumnRenamed("Encrypted","name")
      //.show()


    val windowShell = Window.partitionBy(col("customerId"))

    customerDf.join(salesDF,Seq("customerId"))
      .withColumn("Avg", sum(col("Amount")) over(windowShell))
     // .show()


    val crfdDf = spark.read.options(Map("header"->"true","inferSchema"->"true"))
      .csv("/home/rushi/IdeaProjects/spark_workshop/src/main/resources/databricks/crfd_1365.csv")

    val rdd1  = crfdDf.rdd.map(row => (row.getAs[String]("Member_id"), row))


    /*crfdDf.coalesce(1).write.option("header", "true")
      .option("delimiter", "|").option("compression", "snappy")
      .csv("/home/rushi/IdeaProjects/spark_workshop/src/main/resources/databricks/SNAPPY_FILE_CSV/")
*/




  }
}

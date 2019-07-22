package DataBricks

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.SizeEstimator

object ReadJson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").appName("SG").getOrCreate()
    import spark.implicits._
    val jsonDf = spark.read.option("multiline", "true").json("/home/rushi/IdeaProjects/spark_workshop/src/main/resources/databricks/sample.json")
    jsonDf.printSchema()

    /*jsonDf.withColumn("batters",explode($"batters"))
      .withColumn("batter",$"batters"(0))
      .show(false)
*/
    println("The Size is :-- "+SizeEstimator.estimate(jsonDf))

    //val compSize = jsonDf.write.option("compression", "snappy").parquet("src/main/resources/databricks/parquet")


    println("The Parquet Format Size is :-- "+SizeEstimator.estimate(spark.read.parquet("src/main/resources/databricks/parquet/*.parquet")))


    val sampleDF: DataFrame = Seq(("ABC",1), ("PQR",2), ("XYZ",3)).toDF("Name","id")

    //sampleDF.coalesce(1).write.csv("src/main/resources/jsonDataCSV/")

    spark.read.csv("src/main/resources/jsonDataCSV/")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filePath = "src/main/resources/jsonDataCSV/"
    val fileName = fs.globStatus(new Path(filePath+"part*"))(0).getPath.getName
    fs.rename(new Path(filePath+fileName), new Path(filePath+"sample_file.csv"))



  }

}

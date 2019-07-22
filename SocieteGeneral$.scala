import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object SocieteGeneral {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("SG").getOrCreate()

    val customerDF = spark.read.option("header","true").csv("src/main/resources/sales.csv")
   // customerDF.show()

    //customerDF.select(date_format("date"),"yyyy-MM-dd")

    val x: DataFrame = customerDF.withColumn("date",date_format(lit("date"),"dd-MM-yyyy"))

    val newCustDF = customerDF.withColumnRenamed("date","transdate")
      .withColumn("transdate",col("transdate").cast(DateType))
      .withColumn("transdate",date_format(col("transdate"),"MM/dd/yyyy"))

   newCustDF.withColumn("amount",greatest(col("amount"),col("amount")) as "Greatest" )




  }

}

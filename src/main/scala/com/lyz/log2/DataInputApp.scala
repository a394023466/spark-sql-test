package com.lyz.log2

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object DataInputApp {
/*
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("DataInputApp").master("local[2]").getOrCreate()
    val file = spark.sparkContext.textFile("C:\\Users\\39402\\Desktop\\sparkData.txt")
    val fileRow = file.map(line => {
      val splits = line.split(" ")
      val url = splits(0)
      val ip = splits(1)
      val dateFormat = splits(4) + " " + splits(5)
      val date = dateFormat.substring(dateFormat.indexOf("[") + 1, dateFormat.length - 1)
      val code = splits(9)
      val traffic = splits(10)
      Row(date, url, ip, traffic, code)
    })
    val fileDF = spark.createDataFrame(fileRow, struct)
    val statDF = fileDF.groupBy("ip","code").agg(count("ip").as("times")).orderBy("ip")
    statDF.select(statDF.col("ip"),statDF.col("code"),statDF.col("times"),row_number().over(Window.partitionBy(statDF.col("code")).orderBy(statDF.col("times").desc)).as("times_row_number")).filter("times_row_number<=3").show()
  }
*/

  val struct = StructType(Array(StructField("date", StringType, true), StructField("url", StringType, true),StructField("ip", StringType, true), StructField("traffic", StringType, true), StructField("code", StringType, true)))
}

package com.lyz.log

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object WindowSelect {
  /*def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataInput").config("spark.sql.sources.partitionColumnTypeInference.enabled", "true").master("local[2]")
      .config("spark.sql.parquet.compression.codec", "gzip").getOrCreate()
    /**
      * spark读取文本信息，转换成rdd
      */
    val rdd = spark.sparkContext.textFile("C:\\Users\\39402\\Desktop\\log.txt").map(line => {
      val splits = line.split(" ")
      val date = splits(0)
      val url = splits(1)
      val ip = splits(2)
      Row(date, url, ip)
    })

    /**
      * 将RDD转换成DataFrame
      */
    val df = spark.createDataFrame(rdd, struct)
  }*/

  /**
    * rdd转换成DF所需要的type
    */
  val struct = StructType(Array(StructField("date", StringType), StructField("url", StringType), StructField("ip", StringType)))
}

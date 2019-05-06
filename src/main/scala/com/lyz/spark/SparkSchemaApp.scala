package com.lyz.spark

import org.apache.spark.sql.Row

object SparkSchemaApp {
/*  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.types._
    val spark = SparkSession.builder().appName("SparkSchemaApp").master("local[2]").getOrCreate()

    val fileRDD = spark.sparkContext.textFile("C:\\Users\\39402\\Desktop\\sparkData.txt")

    val rdd = fileRDD.map(line=>Row(line))
    val structType = StructType(Array(StructField("filename",StringType,true)))

    val fileDF = spark.createDataFrame(rdd,structType)

    fileDF.createOrReplaceTempView("file")
    spark.sql("select * from file ").show(false)
  }*/
}

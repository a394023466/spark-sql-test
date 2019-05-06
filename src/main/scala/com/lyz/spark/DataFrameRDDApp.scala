package com.lyz.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Dataframe与RDD互相操作
  */
object DataFrameRDDApp {

/*  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("DataFrameRDDApp").getOrCreate()
    InferReflection(spark)

    //program(spark)


    spark.stop()

  }


  private def program(spark: SparkSession) = {
    val rdd = spark.sparkContext.textFile("C:\\Users\\39402\\Desktop\\peope.txt")
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))


    val structType = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("age", IntegerType, true)))
    val dataFrame = spark.createDataFrame(infoRDD, structType)
    dataFrame.printSchema()
    dataFrame.show()

  }


  private def InferReflection(spark: SparkSession) = {
    val rdd = spark.sparkContext.textFile("C:\\Users\\39402\\Desktop\\peope.txt")
    val dataFrame = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    dataFrame.createOrReplaceTempView("info")

    spark.sql("select * from  info where age >19").show()

    //dataFrame.select(dataFrame.col("age")).show()
  }

  case class Info(id: Int, name: String, age: Int)*/

}

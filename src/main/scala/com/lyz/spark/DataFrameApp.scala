/*
package com.lyz.spark

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession

object DataFrameApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameOperation").master("local[2]").getOrCreate()
    val dataFrame = spark.read.format("json").load("C:\\Users\\39402\\Desktop\\people.json")
    dataFrame.select("name").show()
    dataFrame.select(dataFrame.col("name")).show()
    dataFrame.select(dataFrame.col("name"),(dataFrame.col("age")+10).as("age")).show()
    dataFrame.filter(dataFrame.col("age")>19).show()
    dataFrame.groupBy(dataFrame.col("age")).count().show()
    val rows = dataFrame.where("name='liyuzi' and age=1").select("name").collect()
    //dataFrame.printSchema()
   // dataFrame.show()
    spark.stop()
  }
}
*/

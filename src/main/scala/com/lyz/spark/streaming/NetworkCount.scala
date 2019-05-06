package com.lyz.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkCount {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("NetworkCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val line = ssc.socketTextStream("localhost", 6789)

    val result = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()


    ssc.start()
    ssc.awaitTermination()

  }
}

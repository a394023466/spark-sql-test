package com.lyz.spark

import org.apache.spark.{SparkConf, SparkContext}

object WoldCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:/resources/README.md")
    val words = lines.flatMap(_.split(" ")).filter(word => word != " ")
    val pairs = words.map(word => (word, 1))
    val wordscount = pairs.reduceByKey(_ + _)
    wordscount.foreach(println)
    sc.stop();
  }
}

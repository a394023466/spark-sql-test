package com.lyz

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 类的注释
  *
  * @Package com.lyz
  * @ClassName rdd
  * @Description ${TODO}
  * @Author liyuzhi
  * @Date 2018-07-12 19:26
  */


object rdd {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local").setAppName("rdd")
      val sc = new SparkContext(conf)
    println(sc.parallelize(Array(1, 2, 3, 4)).reduce(_ + _))
  }
}

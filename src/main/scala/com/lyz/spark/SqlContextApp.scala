package com.lyz.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlContextApp {
  def main(args: Array[String]): Unit = {
    //1)创建相应的Context
    val sparkConf = new SparkConf()

    //在测试或者生产中，AppName和Master我们是通过脚本进行指定
    //sparkConf.setMaster("local[2]").setAppName("SparkSql")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //2)相关的处理: json
    val people = sqlContext.read.format("json").load(args(0))
    people.printSchema()
    people.show()



    //3)关闭资源
    sc.stop()
  }
}

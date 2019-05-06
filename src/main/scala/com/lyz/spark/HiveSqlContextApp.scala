package com.lyz.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveSqlContextApp {
  def main(args: Array[String]): Unit = {

    //1)创建相应的Context
    val sparkConf = new SparkConf()

    //在测试或者生产中，AppName和Master我们是通过脚本进行指定
    //sparkConf.setMaster("local[2]").setAppName("SparkSql")

    val sc = new SparkContext(sparkConf)
    val hivecontext = new HiveContext(sc)
    hivecontext.table("students").show()

    sc.stop()
  }
}

package com.lyz.kantanyuan.log

import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 类的注释
  *
  * @Package com.lyz.kantanyuan.log
  * @ClassName MatlogStructType
  * @Description ${TODO}
  * @Author liyuzhi
  * @Date 2018-05-11 23:21
  */


object MatlogStructType {

  def getConnectionStructType: StructType = {
    StructType(Array(
      StructField("date", StringType),
      StructField("protocol", StringType),
      StructField("opt", StringType),
      StructField("process", StringType),
      StructField("client_ip", StringType),
      StructField("client_port", StringType),
      StructField("via_port", StringType),
      StructField("server_ip", StringType),
      StructField("server_port", StringType)
    ))
  }

  def getDisConnectionStructType: StructType = {
    StructType(Array(
      StructField("date", StringType),
      StructField("protocol", StringType),
      StructField("opt", StringType),
      StructField("process", StringType),
      StructField("client_ip", StringType),
      StructField("client_port", StringType)
    ))
  }
}

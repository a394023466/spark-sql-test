package com.lyz.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object KafkaDirect {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("KafkaReceive").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val map = Map("metadata.broker.list" -> "192.168.101.188:9092")
    val topicsSet = Set("test")
    val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, map, topicsSet)

    val p = ".*?18689.*?from:.*?9655".r
    val list = new ArrayBuffer[String]()
    message.map(_._2).foreachRDD(_.foreach(p.findAllMatchIn(_).foreach(line => {
      val splits = line.group(0).split(" ")
      val date = splits(0)
      Row(date)
    })))
    val messageRDD = message.map(_._2).foreachRDD(_.map(line => {
      if (line.matches(".*?18689.*?from:.*?9655")) {
        val splits = line.split(" ")
        val date = splits(0)
        Row(date)
      }
    }))


    list.toParArray
    ssc.start()
    ssc.awaitTermination()


  }
  val struct = StructType(Array(StructField("date", StringType)))

}

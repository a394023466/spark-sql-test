package com.lyz.spark.streaming


import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object KafkaReceive {
  def main(args: Array[String]): Unit = {
    val zkQuorum = "192.168.101.188:2181"
    val groupId = "1"

    val conf = new SparkConf().setAppName("KafkaReceive").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val map = Map("test" -> 1)

    val message = KafkaUtils.createStream(ssc, zkQuorum, groupId, map)
    message.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()

  }
}

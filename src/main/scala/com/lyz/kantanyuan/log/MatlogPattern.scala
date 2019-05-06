package com.lyz.kantanyuan.log

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object MatlogPattern {
  def getConnectionRowRDD(message: RDD[String]): RDD[Row] = {
    val messageRDD = message.map(line => {
      val splits = line.split(" ")

      var date = ""
      var protocol = ""
      var process = ""
      var opt = ""
      var client_ip = ""
      var client_port = ""
      var server_ip = ""
      var server_port = ""
      var via_port = ""
      opt = splits(4)
      if (splits.length == 7 && opt.equals("connection")) {

        date = splits(0) + splits(1)
        protocol = splits(2)

        val processSplits = splits(3).split(",")(0)
        process = processSplits.substring(1, processSplits.length - 1)

        val clientSplits = splits(5).split(":")
        val clientSplits1 = clientSplits(1)
        val clientSplits2 = clientSplits(2)
        val serverSplits = splits(6).split("<=>")
        val serverIp = serverSplits(1).split(":")
        val serverIp0 = serverIp(0)
        val serverIp1 = serverIp(1)

        if (protocol.equals("pop3d")) {
          client_ip = clientSplits1.substring(1, clientSplits1.length - 1)
          client_port = clientSplits2.substring(1, clientSplits2.length - 1)
          server_ip = serverIp0.substring(1, serverIp0.length - 1)
          server_port = serverIp1.substring(1, serverIp1.length - 1)
        } else {
          client_ip = clientSplits1.substring(1)
          client_port = clientSplits2.substring(0, clientSplits2.length - 1)
          server_ip = serverIp0.substring(1)
          server_port = serverIp1.substring(0, serverIp1.length - 1)
        }

        val viaPort = serverSplits(0).split(":")(1)
        via_port = viaPort.substring(1, viaPort.length - 1)

        Row(date, protocol, opt, process, client_ip, client_port, via_port, server_ip, server_port)
      } else {
        opt = ""
        Row(date, protocol, opt, process, client_ip, client_port, via_port, server_ip, server_port)
      }


    })
    messageRDD
  }

  def getDisConnectionRowRDD(message: RDD[String]): RDD[Row] = {
    val messageRDD = message.map(line => {
      val splits = line.split(" ")

      var date = ""
      var protocol = ""
      var process = ""
      var opt = ""
      var client_ip = ""
      var client_port = ""
      opt = splits(4)
      if (opt.equals("disconnection") || opt.equals("disconnected")) {
        date = splits(0) + splits(1)
        protocol = splits(2)
        val processSplits = splits(3).split(",")(0)
        process = processSplits.substring(1, processSplits.length - 1)

        if (protocol.equals("pop3d") || protocol.equals("imapd")) {

          var clientSplits1 = ""
          var clientSplits2 = ""
          if (protocol.equals("pop3d")) {
            val clientSplits = splits(6).split(":")
            clientSplits1 = clientSplits(1)
            clientSplits2 = clientSplits(2)
          }
          if (protocol.equals("imapd")) {
            val clientSplits = splits(5).split(":")
            clientSplits1 = clientSplits(1)
            clientSplits2 = clientSplits(2)
          }

          client_ip = clientSplits1.substring(1, clientSplits1.length - 1)
          client_port = clientSplits2.substring(1, clientSplits2.length - 1)

        } else {
          val clientSplits = splits(5).split(":")
          val clientSplits1 = clientSplits(1)
          val clientSplits2 = clientSplits(2)
          client_ip = clientSplits1.substring(1)
          client_port = clientSplits2.substring(0, clientSplits2.length - 1)
        }

        Row(date, protocol, opt, process, client_ip, client_port)
      } else {
        opt = ""
        Row(date, protocol, opt, process, client_ip, client_port)
      }


    })
    messageRDD
  }
}

package com.lyz.hbase.HbaseService

import java.util.UUID

import com.lyz.hbase.domian.HbaseDomain
import org.apache.avro.SchemaBuilder.ArrayBuilder
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object HbaseService {

  def getHbaseDomain: ListBuffer[HbaseDomain] = {

    val hbaseDomainList = new ListBuffer[HbaseDomain]
    for (i <- 1 to 10) {
      val uuid = UUID.randomUUID().toString
      val hbaseDomain = new HbaseDomain(uuid, "liyuzhi" + i, i)
      hbaseDomainList.append(hbaseDomain)
    }
    hbaseDomainList
  }

  def getHbaseDomainArray: ListBuffer[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = {
    val domains = getHbaseDomain
    val array = new ListBuffer[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])]
    domains.foreach(domain => {
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(domain.name)))))
      array.append((Bytes.toBytes(domain.id), Array((Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(domain.age)))))
    })
    array
  }
}

package com.lyz.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext

object HbaseBatch1 {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "test")
    //获取配置文件的属性，为连接Hbase提供参数
    val configuraton = HBaseConfiguration.create
    val hbaseContext = new HBaseContext(sc, configuraton)

    val tableNameStr = "user"
    val loadPathStr = "/hbase/data1"

    //连接与Hbase建立连接
    val conn = ConnectionFactory.createConnection(configuraton)
    //获取Hbase表
    val table = conn.getTable(TableName.valueOf(tableNameStr))

    println(conn.getAdmin.tableExists(table.getName))
    val rdd = sc.parallelize(HbaseService.HbaseService.getHbaseDomainArray)

    rdd.hbaseBulkLoad(hbaseContext, TableName.valueOf(tableNameStr),
      t => {
        val rowKey = t._1
        val family: Array[Byte] = t._2(0)._1
        val qualifier = t._2(0)._2
        val value = t._2(0)._3
        val keyFamilyQualifier = new KeyFamilyQualifier(rowKey, family, qualifier)
        Seq((keyFamilyQualifier, value)).iterator
      },
      loadPathStr)

    val load = new LoadIncrementalHFiles(configuraton)
    load.doBulkLoad(new Path(loadPathStr), conn.getAdmin, table, conn.getRegionLocator(TableName.valueOf(tableNameStr)))
  }
}

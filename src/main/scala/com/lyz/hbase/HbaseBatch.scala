package com.lyz.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable.ListBuffer

object HbaseBatch {
  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().appName("DataInput").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val tableNameStr = "user"

    //获取配置文件的属性，为连接Hbase提供参数
    val configuraton = HBaseConfiguration.create

    //连接与Hbase建立连接
    val conn = ConnectionFactory.createConnection(configuraton)

    //获取Hbase的表的名字
    val tableName = TableName.valueOf(tableNameStr)
    //根据表的名字来得到表
    val table = conn.getTable(tableName)

    //设定HBase的输入表
    configuraton.set(TableInputFormat.INPUT_TABLE, tableNameStr)

    //初始化一个Job
    val job = Job.getInstance(configuraton)

    //设置输出的Key类型
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])

    //设置输出值的类型
    job.setMapOutputValueClass(classOf[KeyValue])

    val regionLocator = conn.getRegionLocator(tableName)
    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    val array = Array(1 to 10: _*)
    val value = sc.parallelize(array)
    val listKeyValue = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
    val rdd = value.map(x => {
      val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "info".getBytes(), "aa1".getBytes(), "value_xxx".getBytes())
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(x), "info".getBytes(), "aa2".getBytes(), "value_xxx".getBytes())
      val kv2: KeyValue = new KeyValue(Bytes.toBytes(x), "info".getBytes(), "aa3".getBytes(), "value_xxx".getBytes())
      val kv3: KeyValue = new KeyValue(Bytes.toBytes(x), "info".getBytes(), "aa4".getBytes(), "value_xxx".getBytes())
      val kv4: KeyValue = new KeyValue(Bytes.toBytes(x), "info".getBytes(), "aa5".getBytes(), "value_xxx".getBytes())
      val kv5: KeyValue = new KeyValue(Bytes.toBytes(x), "info".getBytes(), "aa6".getBytes(), "value_xxx".getBytes())
      listKeyValue.append((new ImmutableBytesWritable(Bytes.toBytes(x)), kv))
      listKeyValue.append((new ImmutableBytesWritable(Bytes.toBytes(x)), kv1))
      listKeyValue.append((new ImmutableBytesWritable(Bytes.toBytes(x)), kv2))
      listKeyValue.append((new ImmutableBytesWritable(Bytes.toBytes(x)), kv3))
      listKeyValue.append((new ImmutableBytesWritable(Bytes.toBytes(x)), kv4))
      listKeyValue.append((new ImmutableBytesWritable(Bytes.toBytes(x)), kv5))
    })


    val kvRDD = sc.makeRDD(listKeyValue).map(kv => {

      (kv._1, kv._2)

    })
    println(kvRDD)
    //数据转换成HFile，导入到HDFS目录中
    kvRDD.saveAsNewAPIHadoopFile("/hbase/data1", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], configuraton)


    //将HDFS上的HFile导入到Hbase的表中
    val bulkLoader = new LoadIncrementalHFiles(configuraton)
    bulkLoader.doBulkLoad(new Path("/hbase/data1"), conn.getAdmin, table, regionLocator)*/
  }
}

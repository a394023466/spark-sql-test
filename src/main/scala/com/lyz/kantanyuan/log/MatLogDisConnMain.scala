package com.lyz.kantanyuan.log

import java.net.URI

import com.lyz.kantanyuan.log.hdfs.HdfsProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object MatLogDisConnMain {
  def main(args: Array[String]): Unit = {

    val HDFS_FILE_INPUT_PATH = "hdfs://hadoop001:8020/opt/testfile/mta.log.20150528.14328288010.74891400"
    val HDFS_FILE_OUTPUT_PATH = "hdfs://hadoop001:8020/opt/testfile/output/disconn"


    val spark = SparkSession.builder().appName("MatLogDisConnMain").getOrCreate()

    val connMsg = spark.sparkContext.textFile(HDFS_FILE_INPUT_PATH)

    val disconnRDD: RDD[Row] = MatlogPattern.getDisConnectionRowRDD(connMsg)

    val hdfs = FileSystem.get(new URI(HdfsProperties.HDFS_URL), new Configuration(), "root")

    val path = new Path(HDFS_FILE_OUTPUT_PATH)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)

    }
    val df = spark.createDataFrame(disconnRDD, MatlogStructType.getDisConnectionStructType)
    df.filter(df.col("opt").=!=("")).write.format("csv").save(HDFS_FILE_OUTPUT_PATH)
  }
}
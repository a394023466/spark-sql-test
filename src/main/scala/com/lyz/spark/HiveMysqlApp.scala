package com.lyz.spark

object HiveMysqlApp {
 /* def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("HiveMysqlApp").master("local[2]").getOrCreate()
    val hiveDF = spark.table("t")
    val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/hive").option("dbtable", "hive.TBLS").option("user", "root").option("password", "123456").option("driver", "com.mysql.jdbc.Driver").load()


    hiveDF.join(jdbcDF, hiveDF.col("name") === jdbcDF.col("age")).show()

  }*/
}

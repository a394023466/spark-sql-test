package com.lyz.util

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.hbase.client.Put

object JdbcUtil {


  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sql_table?user=root&password=12345&zeroDateTimeBehavior=convertToNull&characterEncoding=utf8&serverTimezone=GMT&useSSL=false")
  }




  def close(conn: Connection, pst: PreparedStatement): Unit = {
    try {
      if (pst != null) {
        pst.close()
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }
}

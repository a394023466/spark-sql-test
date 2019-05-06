package com.lyz.log

import com.lyz.util.JdbcUtil

import scala.collection.mutable.ListBuffer

object DataInsertMysql {

  def insertDate(list: ListBuffer[Data]): Unit = {
    val connection = JdbcUtil.getConnection()


    //关闭自动提交
    connection.setAutoCommit(false)
    val statement = connection.prepareStatement("insert into sql_table (date,url,ip) values(?,?,?)")

    /**
      * 一下是scala遍历方式
      * 利用jdbc插入mysql数据库
      */
    for (ele <- list) {
      statement.setString(1, ele.date)
      statement.setString(2, ele.url)
      statement.setString(3, ele.ip)

      //将statement放到批处理里
      statement.addBatch()
    }
    //执行批处理
    statement.executeBatch()

    //提交
    connection.commit()
  }
}

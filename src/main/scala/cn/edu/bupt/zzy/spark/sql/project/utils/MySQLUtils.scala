package cn.edu.bupt.zzy.spark.sql.project.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL操作工具类
  */
object MySQLUtils {

  /**
    * 获取数据库连接
    */
  def getConnection() = {
    //DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=root")
    DriverManager.getConnection("jdbc:mysql://localhost/imooc_project?user=root&password=1q2w3e4r&useUnicode=true&useSSL=false&characterEncoding=UTF-8&serverTimezone=UTC")
  }

  /**
    * 释放数据库连接等资源
    */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}

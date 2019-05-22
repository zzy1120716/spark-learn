package cn.edu.bupt.zzy.spark.streaming

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("hadoop000", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    //state.print() // 此处仅仅是将统计结果输出到控制台

    // TODO... 将结果写入到MySQL
    /*result.foreachRDD(rdd => {
      val connection = createConnection()   // executed at the driver
      rdd.foreach { record =>
        val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
        connection.createStatement().execute(sql)
      }
    })*/

    result.print()

    result.foreachRDD(rdd => {

      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()

        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })

        connection.close()
      })

    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 获取MySQL的连接
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark", "root", "root")
  }

}

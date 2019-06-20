package cn.edu.bupt.zzy.spark.structured

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 事件时间窗口操作示例
  * Usage: StructuredNetworkWordCountWindowed <hostname> <port> <window duration> [<slide duration>]
  * 其中slide duration是连续窗口之间的偏移量，它应该小于等于window duration
  * 推荐：window duration = 10, slide duration = 5
  */
object StructuredNetworkWordCountWindowed {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: StructuredNetworkWordCountWindowed <hostname> <port>" +
      " <window duration in seconds> [<slide duration in seconds>]")
      System.exit(1)
    }

    val host = args(0)
    val port = args(1)
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3) windowSize else args(3).toInt
    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val spark = SparkSession
      .builder()
      .appName("StructuredNetworkWordCountWindowed")
      .getOrCreate()

    import spark.implicits._

    // 创建DataFrame表示来自连接到host:port的输入行的流
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    // 将行分割成单词，保留时间戳
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // 将数据通过窗口和单词分组，并计算每一组的数量
    val windowedCounts = words.groupBy(
      // 需引入sql函数的依赖：import org.apache.spark.sql.functions._
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().orderBy("window")

    // 开始运行查询，打印窗口聚合的word count到控制台
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }

}

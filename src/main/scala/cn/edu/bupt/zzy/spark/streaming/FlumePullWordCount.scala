package cn.edu.bupt.zzy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming整合Flume的第二种方式
  */
object FlumePullWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: FlumePullWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    // 生产环境通过spark-submit参数给出master URL和AppName
    val sparkConf = new SparkConf() //.setMaster("local[2]").setAppName("FlumePushWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // TODO... 如何使用SparkStreaming整合Flume
    val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)

    // 获取到内容，trim函数去掉空格
    flumeStream.map(x => new String(x.event.getBody.array()).trim)
        .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }

}

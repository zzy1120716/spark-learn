package cn.edu.bupt.zzy.spark.streaming.project.spark

import cn.edu.bupt.zzy.spark.streaming.project.domain.ClickLog
import cn.edu.bupt.zzy.spark.streaming.project.utils.DateUtils
import cn.edu.bupt.zzy.spark.streaming.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import cn.edu.bupt.zzy.spark.streaming.project.domain.{CourseClickCount, CourseSearchClickCount}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.println("Usage: ImoocStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp") //.setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    // 测试步骤一：测试数据接收
    //messages.map(_._2).count().print()

    // 测试步骤二：数据清洗
    // 124.143.132.98  2019-05-06 16:02:01     "GET /class/112.html HTTP/1.1"  200     https://search.yahoo.com/search?p=Hadoop基础
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {
      val infos = line.split("\t")

      // infos(2) = "GET /class/112.html HTTP/1.1"
      // url = /class/112.html
      val url =  infos(2).split(" ")(1)
      var courseId = 0

      // 把实战课程的课程编号拿到了
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    //cleanData.print()

    // 测试步骤三：统计今天到现在为止实战课程的访问量
    cleanData.map(x => {

      // HBase rowkey设计：20181111_88

      // map:
      // (20181111_88, 1)(20181111_88, 1)(20181111_88, 1)
      // (20181111_77, 1)(20181111_77, 1)
      (x.time.substring(0, 8) + "_" + x.courseId, 1)

      // reduceByKey:
      // (20181111_88, 3)(20181111_77, 2)

      // foreachRDD: reduceByKey返回一个DStream，拿到这个DStream中所有的RDD

    }).reduceByKey(_ + _).foreachRDD(rdd => {

      // foreachPartition: 每个partition写一次，性能更好
      // 参考之前的例子：ForeachRDDApp.scala
      rdd.foreachPartition(partitionRecords => {

        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseClickCount(pair._1, pair._2))
        })

        // 每个partition保存一次，到HBase
        CourseClickCountDAO.save(list)
      })
    })

    // 测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量
    cleanData.map(x => {

      //https://www.sogou.com/web?query=Spark SQL实战
      //
      // ==>
      //
      //https:/www.sogou.com/web?query=Spark SQL实战
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")
      var host = ""

      if (splits.length > 2) {
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x => {

      (x._3.substring(0, 8) + "_" + x._1 + "_" + x._2, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => { // 与上一个功能一样

      rdd.foreachPartition(partitionRecords => {

        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {
          list.append(CourseSearchClickCount(pair._1, pair._2))
        })

        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}

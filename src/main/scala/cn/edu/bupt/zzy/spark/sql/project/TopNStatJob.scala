package cn.edu.bupt.zzy.spark.sql.project

import cn.edu.bupt.zzy.spark.sql.project.dao.StatDAO
import cn.edu.bupt.zzy.spark.sql.project.model.DayVideoAccessStat
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * TopN统计Spark作业
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TopNStatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("C:\\Users\\zzy\\data\\clean")

    accessDF.printSchema()
    accessDF.show(false)

    // 最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF)

    spark.stop()
  }

  /**
    * 最受欢迎的TopN课程
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {

    /**
      * DataFrame方式
      */
//    import spark.implicits._
//
//    val videoAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
//      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
//
//    videoAccessTopNDF.show(false)

    /**
      * SQL方式
      */
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopNDF = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      "where day='20170511' and cmsType='video' " +
      "group by day, cmsId order by times desc")

    videoAccessTopNDF.show(false)

    /**
      * 将统计结果写入到MySQL中
      */
    try {
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)

      })
    } catch {
      case e:Exception => e.printStackTrace()
    }

  }

}

package cn.edu.bupt.zzy.spark.sql.project

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions._

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
  }

}
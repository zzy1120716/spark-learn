package cn.edu.bupt.zzy.spark.sql.project.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成数据清洗操作
  */
object SparkStatCleanJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    // C:\Users\zzy\data\access.log
    val accessRDD = spark.sparkContext.textFile("file:///C:\\Users\\zzy\\data\\access.log")

    //accessRDD.take(10).foreach(println)

    // RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)

    //accessDF.printSchema()
    //accessDF.show(false)

    accessDF.coalesce(1).write.format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("day")
      .save("file:///C:\\Users\\zzy\\data\\clean")

    spark.stop()
  }

}

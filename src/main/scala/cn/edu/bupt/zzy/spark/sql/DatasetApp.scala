package cn.edu.bupt.zzy.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Dataset操作
  */
object DatasetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    // 注意：需要导入隐式转换
    import spark.implicits._

    val path = "file:///C:\\Users\\zzy\\data\\sales.csv"
    // spark如何解析csv文件?
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.show

    val ds = df.as[Sales]
    ds.map(line => line.itemId).show

    // sql：运行阶段才报错
    //spark.sql("seletc name from person").show

    // df：
    // 方法名写错：不能通过编译
    //df.seletc("name")   // compile no
    // 字段名写错：运行阶段报错
    //df.select("nname")  // compile ok

    // ds：
    // 字段名写错：不能通过编译
    //ds.map(line => line.itemid)  // compile no

    spark.stop()
  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)
}

package cn.edu.bupt.zzy.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    /**
      * 创建StreamingContext需要两个参数：SparkConf和batch interval
      */
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /**
      * 构建黑名单
      */
    val blacks = List("zs", "ls")
    // (zs:true)(ls:true)
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x=>(x,true))

    // 20180808,zs
    // 20180808,ls
    // 20180808,ww
    val lines = ssc.socketTextStream("hadoop000", 6789)

    // 1. (zs:20180808,zs)(ls:20180808,ls)(ww:20180808,ww)
    // 2. leftjoin
    // (zs:[<20180808,zs>,<true>])		x
    // (ls:[<20180808,ls>,<true>])		x
    // (ww:[<20180808,ww>,<false>])	==> tuple 1
    val clickLog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })

    // output: 20180808,ww
    clickLog.print()

    ssc.start()
    ssc.awaitTermination()

  }

}

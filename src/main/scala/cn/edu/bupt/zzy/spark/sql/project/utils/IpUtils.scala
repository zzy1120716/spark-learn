package cn.edu.bupt.zzy.spark.sql.project.utils

import com.ggstar.util.ip.IpHelper

/**
  * IP解析工具类
  */
object IpUtils {

  def getCity(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("58.30.15.255"))
    println(getCity("218.75.35.226"))
  }

}

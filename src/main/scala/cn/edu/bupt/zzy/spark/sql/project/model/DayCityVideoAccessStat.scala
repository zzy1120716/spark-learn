package cn.edu.bupt.zzy.spark.sql.project.model

/**
  * 每天各地市访问统计实体类
  */
case class DayCityVideoAccessStat(day: String, cmsId: Long, city: String, times: Long, timesRank: Int)

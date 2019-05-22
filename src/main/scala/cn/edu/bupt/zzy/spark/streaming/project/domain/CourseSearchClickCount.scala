package cn.edu.bupt.zzy.spark.streaming.project.domain

/**
  * 从搜索引擎过来的实战课程点击数实体类
  * @param day_search_course  对应的就是HBase中的rowkey，20181111_www.sogou.com_1
  * @param click_count 对应的20181111_www.sogou.com_1的访问总数
  */
case class CourseSearchClickCount(day_search_course:String, click_count:Long)

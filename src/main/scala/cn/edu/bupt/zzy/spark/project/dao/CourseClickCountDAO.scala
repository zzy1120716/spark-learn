package cn.edu.bupt.zzy.spark.project.dao

import cn.edu.bupt.zzy.spark.project.domain.CourseClickCount
import cn.edu.bupt.zzy.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击数 - 数据访问层
  */
object CourseClickCountDAO {

  val tableName = "my_course_clickcount"
  val cf = "info"
  val qualifier = "click_count"

  /**
    * 保存数据到HBase
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    for (ele <- list) {

      // 在已有的值的基础上增加
      table.incrementColumnValue(
        Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifier),
        ele.click_count)
    }

  }

  /**
    * 根据rowkey查询值
    */
  def count(day_course: String): Long = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(Bytes.toBytes(day_course))
    val value = table.get(get).getValue(cf.getBytes, qualifier.getBytes)

    // 第一次操作值是没有的，要做判断
    if (value == null) {  // Scala中equals和“==”操作是等价的
      0L
    } else {
      Bytes.toLong(value)
    }

  }

  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20181111_8", 8))
    list.append(CourseClickCount("20181111_9", 9))
    list.append(CourseClickCount("20181111_1", 100))

    save(list)

    println(count("20181111_8") + " : " + count("20181111_9") + " : " + count("20181111_1"))
  }

}

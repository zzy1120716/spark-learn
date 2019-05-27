package cn.edu.bupt.zzy.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * DataFrame中的其他操作
  */
object DataFrameCase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    // RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///C:\\Users\\zzy\\data\\student.data")

    // 注意：需要导入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    // 21||1-711-710-6552|lectus@aliquetlibero.co.uk

    // show默认只显示前20条，可以设定显示条数，以及超出部分是否以“...”进行显示
    studentDF.show()
    studentDF.show(30)
    studentDF.show(30, false)

    // 取前n条记录，n = 10, 1, 3
    studentDF.take(10)
    studentDF.take(10).foreach(println)
    studentDF.first()
    studentDF.head(3)

    // 查询指定的列
    studentDF.select("email").show(30, false)
    studentDF.select("name", "email").show(30, false)

    // 过滤出name字段为空字符串或NULL的行
    studentDF.filter("name=''").show
    studentDF.filter("name='' OR name='NULL'").show
    studentDF.filter("name!='' AND name!='NULL'").show

    // name以M开头的人
    studentDF.filter("SUBSTR(name,0,1)='M'").show

    // 排序（升/降序）
    studentDF.sort(studentDF("name")).show
    studentDF.sort(studentDF("name").desc).show

    // 名字相同的按id升序排列
    studentDF.sort("name", "id").show

    // 名字升序，id降序排列
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show

    // 修改显示列名
    studentDF.select(studentDF("name").as("student_name")).show

    // join操作
    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    studentDF.join(studentDF2, studentDF.col("id") === studentDF2.col("id")).show

    spark.stop()
  }

  case class Student(id: Int, name: String, phone: String, email: String)

}

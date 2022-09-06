package cn.doitedu.day09

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.beans.BeanProperty

/**
 * 创建DataFrame
 *  1.RDD + case class ,在调用toDF
 *  2.RDD + 普通的class
 */
object C02_DataFrameDemo2 {

  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameDemo2")
      .master("local[*]")
      .getOrCreate()

    //2.创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("data/user.txt")
    //2将数据封装到普通的class中
    val boyRDD: RDD[Boy2] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      new Boy2(name, age, fv) //字段名称，字段的类型
    })

    //3.将RDD和Schema进行关联
    val df = spark.createDataFrame(boyRDD, classOf[Boy2])

    //df.printSchema()

    //4.使用DSL风格的API
    import spark.implicits._
    val df2: Dataset[Row] = df
      .orderBy($"fv" desc, $"age" asc)
      .select($"name", $"fv", $"age")
    df2.show()

    spark.stop()


  }
}

//参数前面必须有var或val
//必须添加给字段添加对应的getter方法，在scala中，可以@BeanProperty注解
class Boy2(
            @BeanProperty
            val name: String,
            @BeanProperty
            val age: Int,
            @BeanProperty
            val fv: Double) {

//  def getName(): String = {
//    this.name
//  }


}
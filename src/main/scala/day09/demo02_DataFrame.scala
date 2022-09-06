package day09

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.beans.BeanProperty

/**
 * 创建DataFrame
 * 1.RDD + case class ,再调用toDF
 * 2.RDD + 普通的class
 */
object demo02_DataFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrame02")
      .master("local[*]")
      .getOrCreate()

    //2.创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("data/user.txt")
    val user2RDD: RDD[Users2] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      new Users2(name, age, fv)
    })

    //3.将RDD和schema关联
    //Users2的get方法,确定了schema的名称及类型
    val df: DataFrame = spark.createDataFrame(user2RDD, classOf[Users2])
    df.printSchema()
    //df.show()
    //4.使用DSL风格的API
    import spark.implicits._
    val df2 = df
      .orderBy($"fv" desc, $"age" asc)
      .select($"name", $"age", $"fv")
    df2.show()
    spark.stop()
  }
}

//参数前面必须有var或val
//必须添加给字段添加对应的getter方法，在scala中，可以@BeanProperty注解
class Users2(
              @BeanProperty
              val name: String,
              @BeanProperty
              val age: Int,
              @BeanProperty
              val fv: Double
            ) {

}



package day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 演示spark sql 新的api,即DataFrame的创建即使用
 *
 * DataFrame是对RDD的增强,也是一个分布式数据集
 *
 * DataFrame = RDD + Schema(额外的描述信息)
 *
 * DataFrame有两种编程API
 *   1.支持SQL风格的API
 *   2.支持DSL编程风格的API
 */
object demo01_DataFrame {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //2.创建DataFrame
    //2.1线创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("data/user.txt")

    //2.2创建Schema
    //对数据整理并且关联Schema
    val users: RDD[Users] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      Users(name, age, fv)
    })
    //2.3将RDD关联Schema,得到DataFrame
    //导入隐士转换,才可以调用toDF方法
    import spark.implicits._
    val df: DataFrame = users.toDF()
    //打印DateFrame的Schema信息
    df.printSchema()
    //3.将DataFrame注册成表
    df.createTempView("v_users")

    //4.写sql
    val df2: DataFrame = spark.sql("select * from v_users order by fv desc ,age asc")

    //5.触发Action
    df2.show()

    //6.释放资源
    spark.stop()

  }
}
case class Users (name:String, age: Int, fv:Double)
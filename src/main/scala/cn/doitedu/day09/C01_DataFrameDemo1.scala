package cn.doitedu.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 演示SparkSQL新的的编程API，即DataFrame的创建可以使用
 *
 * DataFrame是对RDD的增强，也是一个分布式数据集
 *
 * DataFrame = RDD + Schema(额外的描述信息)
 *
 * DataFrame有两种编程API
 *   1.支持SQL风格的API
 *   2.支持DSL编程风格的API
 *
 *
 */
object C01_DataFrameDemo1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")
    //1.SparkSession，是对SparkContext的增强
    val session: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    //2.创建DataFrame
    //2.1先创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("data/user.txt")
    //2.2对数据进行整理并关联Schema
    val tfBoy: RDD[Boy] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      Boy(name, age, fv) //字段名称，字段的类型
    })

    //2.3将RDD关联schema，将RDD转成DataFrame
    //导入隐式转换
    import session.implicits._
    val df: DataFrame = tfBoy.toDF
    //打印DataFrame的Schema信息
    df.printSchema()

    //3.将DataFrame注册成视图（虚拟的表）
    df.createTempView("v_users")

    //4.写sql（Transformation）
    val df2: DataFrame = session.sql("select * from v_users order by fv desc, age asc")

    //5.触发Action
    df2.show()

    //6.释放资源
    session.stop()




  }

}

case class Boy(name: String, age: Int, fv: Double)
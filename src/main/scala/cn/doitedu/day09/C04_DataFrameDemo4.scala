package cn.doitedu.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * 创建DataFrame的第四种方式
 *
 *   RDD[TupleN] + Schema = DataFrame
 *
 */

object C04_DataFrameDemo4 {

  def main(args: Array[String]): Unit = {


    //1.创建SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameDemo2")
      .master("local[*]")
      .getOrCreate()

    //2先创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("data/user.txt")
    //3.1对RDD进行整理，将数据封装到Tuple中
    val tpRdd: RDD[(String, Int, Double)] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      (name, age, fv) //字段的个数，字段的类型，字段的名称_1, _2, _3
    })

    //导入隐式转换
    import spark.implicits._
    //给对应的字段重新命名
    val df = tpRdd.toDF("name", "age", "fv")

    df.printSchema()

    df.show()

    spark.stop()




  }

}

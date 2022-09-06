package cn.doitedu.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 *
 * 创建DataFrame的第三中方式
 *
 *   RDD[Row] + Schema = DataFrame
 *
 */

object C01_DataFrameDemo2 {

  def main(args: Array[String]): Unit = {


    //1.创建SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameDemo2")
      .master("local[*]")
      .getOrCreate()

    //2先创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("data/user.txt")
    //3.1对RDD进行整理，将数据封装到Row
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      Row(name, age, fv) //字段的个数
    })

    //3.2创建Schema（字段名称，字段类型）
    val schema: StructType = StructType(
      Seq(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("fv", DoubleType, false),
      )
    )

    //4.将RDD[Row]关联schema
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)

    df.printSchema()

    df.show()

    spark.stop()




  }

}

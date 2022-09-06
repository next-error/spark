package day09

import org.apache.parquet.format.IntType
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

/**
 * 创建DataFrame的第三种方式
 *
 * RDD[Row] + Schema = DataFrame
 */
object demo03_DataFrame {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()


    //2.先创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("data/user.txt")

    //3.对RDD整理,将数据封装到Row
    val rowRow: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      Row(name, age, fv)// 只知道字段个数,不知道名称和类型,需要创建Schema
    })
    //3.2创建Schema,指定字段名称及类型
    val schema:StructType  = StructType(
      Seq(
        StructField("name",StringType) ,
          StructField("age",IntegerType),
          StructField("fv",DoubleType),
      )
    )

    //4.将RDD与schema关联
    val df: DataFrame = spark.createDataFrame(rowRow, schema)
    df.printSchema()
    df.show()
    //5.释放资源
    spark.stop()

  }
}

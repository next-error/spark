package cn.doitedu.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object C05_EncoderTest1 {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    //创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("data/user.txt")

    //对数据进行整理，将数据封装到Row中
    //Row里面可以装多个字段，不知道字段的名称和类型
    val rowRDD: RDD[Row] = lines.map(e => {
      val fields = e.split(",")
      val f1 = fields(0)
      val f2 = fields(1).toInt
      val f3 = fields(2).toDouble
      Row(f1, f2, f3)
    })

    //创建Schema（字段名称和字段类型，这两个必须有），可选的字段能不能为空
    val schema = new StructType()
      .add("name", StringType, true)
      .add("age", IntegerType)
      .add("fv", DoubleType)

    //将RDD[Row]在关联Schema，就变成了DataFrame
    //createDataFrame方法中，指定了，Encoder（序列化器，是个schema管理起来的），和执行计划
    val userDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    userDF.printSchema()

    userDF.show()
  }
}

package cn.doitedu.day11

import java.lang

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object C06_EncoderTest2 {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    //创建Dataset，Dataset[String]默认有schema，字段名称为value，字段类型为string
    val lines: Dataset[String] = spark.read.textFile("data/user.txt")

    //创建Scheme（字段名称、字段类型）
    val schema = new StructType()
      .add("name", StringType)
      .add("age", IntegerType)
      .add("fv", DoubleType)

    //Encoder = scheame + 序列化方式
    //Row + schema +  + 序列化方式 = RowEncoder
    //import spark.implicits._
    implicit val rowEncoder: ExpressionEncoder[Row] = RowEncoder(schema)

    //对Dataset进行map操作
    //lines就是Dataset，对Dataset进行map转换，返回的还是Dataset
    //原来Dataset中对应的数据类型为String，现在转换后，对应的数据类型为Row
    val rowDF: Dataset[Row] = lines.map(e => {
      //对数据进行切分
      val fields = e.split(",")
      val f1 = fields(0)
      val f2 = fields(1).toInt
      val f3 = fields(2).toDouble
      //然后将数据转成对应的类型，封装到Row中
      Row(f1, f2, f3)
    })

    rowDF.printSchema()
    rowDF.show()


  }
}

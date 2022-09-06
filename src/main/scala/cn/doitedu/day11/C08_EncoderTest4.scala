package cn.doitedu.day11

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.beans.BeanProperty

object C08_EncoderTest4 {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    //创建Dataset，Dataset[String]默认有schema，字段名称为value，字段类型为string
    val lines: Dataset[String] = spark.read.textFile("data/user.txt")

    //Encoder = scheame + 序列化方式
    //Tuple + schema +  + 序列化方式 = TupleEncoder
    //有所有基本类型的隐式转换
    //import spark.implicits._
    implicit val teacherEncoder: Encoder[Teacher] = Encoders.bean(classOf[Teacher])
    //没有找到Teacher类型的Encoder，即Encoder[Teacher]
    val rowDF: Dataset[Teacher] = lines.map(e => {
      //对数据进行切分
      val fields = e.split(",")
      val f1 = fields(0)
      val f2 = fields(1).toInt
      val f3 = fields(2).toDouble
      //然后将数据转成对应的类型，封装到Row中
      new Teacher(f1, f2, f3)
    })

    rowDF.printSchema()
    rowDF.show()

  }
}

class Teacher(
               @BeanProperty
               var name: String,
               @BeanProperty
               var age: Int,
               @BeanProperty
               var fv: Double
             ){

}
package cn.doitedu.day11

import cn.doitedu.day05.IpUtils
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

/**
 * 实现一个自定义UDAF（User Defined Aggregate Function）
 *
 * select avg(age) avg_age from v_user3 group by gender
 *
 *
 */
object C03_UDFDemo3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("C01_UDFDemo1").master("local[*]").getOrCreate()


    val df = spark.read
        .option("header", "true")
      .csv("data/user3.txt")

    df.createTempView("v_user_3")

    import org.apache.spark.sql.functions._
    //udaf是定义在org.apache.spark.sql.functions下的一个函数
    //udaf(new MyAvgFunction()),将你自定义的Aggregator转成UserDefinedFunction
    //1.注册函数
    spark.udf.register("my_avg", udaf(new MyAvgFunction()))

    //使用spark自带的内置函数
    val res = spark.sql(
      """
        |select
        |  gender,
        |  my_avg(age) avg_age
        |from
        |  v_user_3
        |group by
        |  gender
        |
        |""".stripMargin)


    res.show()

  }

}

class MyAvgFunction extends Aggregator[Int, (Int, Int), Double] {

  //指定每个分区每个组的初始值
  override def zero: (Int, Int) = {
    (0, 0) //（年龄的初始值，参与运算的人数）
  }

  //再每个分区，每个组，输入一条数据，调用一次reduce方法(上游完成的)
  override def reduce(buff: (Int, Int), age: Int): (Int, Int) = {
    (buff._1 + age, buff._2 + 1)
  }

  //将每个分区，每个组返回的结果进行聚合
  override def merge(b1: (Int, Int), b2: (Int, Int)): (Int, Int) = {
    (b1._1 + b2._1, b1._2 + b2._2)
  }

  //计算最终返回的结果
  override def finish(reduction: (Int, Int)): Double = {
    reduction._1 / reduction._2
  }

  //指定数据的编解码的方式（溢写磁盘、网络传输）
  override def bufferEncoder: Encoder[(Int, Int)] = {
    Encoders.tuple(Encoders.scalaInt, Encoders.scalaInt)
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}


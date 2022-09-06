package day11

import org.apache.spark.sql.SparkSession

/**
 * Spark Sql的UDF (User Defined FUnction)
 * UDF功能: Spark内置函数无法满足业务要求,而且还不想在sql中混搭DSL风格的API及RDD
 *
 * 根据业务需求自己定义函数,以后可以在sql中调用自己定义的函数
 *
 * UDF分类:
 * 1.UDF: 1进1出
 * 2.UDAF: 多进1出 (一个组返回一个结果)
 * 3.UDPF: 1进多出 (类似于explode炸裂函数)
 */
object demo01_UDF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val df = spark.read.csv("data/user.txt")
    val df2 = df.toDF("name", "age", "fv", "province", "city")
    df2.createTempView("v_log")
    /*val df3 = spark.sql(
      """
        |
        |select
        |  name,
        |  age,
        |  fv,
        |  concat_ws('-', province, city) location
        |from
        |  v_log
        |""".stripMargin
    )*/

    //自己写一个类似于concat_ws的函数
      //1.注册一个自定义函数
    spark.udf.register("my_concat_ws", (sp: String, province:String, city:String) =>{
      province + sp + city

    })
      //2.调用

    val df3 = spark.sql(
      """
        |
        |select
        |  name,
        |  age,
        |  fv,
        |  my_concat_ws('-', province, city) location
        |from
        |  v_log
        |""".stripMargin
    )
    df3.show()
    spark.stop()


  }
}

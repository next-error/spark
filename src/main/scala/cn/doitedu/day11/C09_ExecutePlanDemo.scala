package cn.doitedu.day11

import org.apache.spark.sql.SparkSession

/**
 * 演示执行计划
 */
object C09_ExecutePlanDemo {

  def main(args: Array[String]): Unit = {


    //创建SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val df1 = spark.read.json("data/order2.json")
    //按照日期进行分区，并以parquet格式写入到HDFS中
    df1.write.partitionBy("dt").parquet("data/par-orders")

    val df2 = spark.read.parquet("data/par-orders")

    import spark.implicits._
    val df3 = df2.select("category_id", "money", "status", "oid", "dt")
      .where($"dt" === "2020-01-25" and $"money" >= 25)
      .select("oid", "dt", "money")

    //打印执行计划
    //Parsed Logical Plan -> Analyzed Logical Plan -> Optimized Logical Plan -> Physical Plan
    df3.explain(true)

    //df3.show()

  }
}

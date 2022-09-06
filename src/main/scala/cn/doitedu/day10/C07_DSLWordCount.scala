package cn.doitedu.day10

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 使用DSL风格的API编写WordCount
 */
object C07_DSLWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C07_DSLWordCount")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("data/words.txt")
    import spark.implicits._
    val words = lines.flatMap(_.split(" "))
    //val res = words.groupBy($"value").count().orderBy($"count".desc)

    //导入sparksql用到的函数
    import org.apache.spark.sql.functions._
    val res = words.withColumnRenamed("value", "word")
      .groupBy("word")
      .agg(count("*") as "counts")
      .orderBy($"counts".desc)

    res.show()

  }

}

package cn.doitedu.day10

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 使用DSL风格的API编写WordCount
 */
object C07_SQLWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C07_DSLWordCount")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("data/words.txt")
    //lines.printSchema()
    //lines.show()
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    //使用sql方式
    words.createTempView("v_words")
    val res = spark.sql(
      """
        |select
        |  value word,
        |  count(*) counts
        |from
        |  v_words
        |group by
        |  value
        |order by
        |  counts desc
        |""".stripMargin)


    res.show()

  }

}

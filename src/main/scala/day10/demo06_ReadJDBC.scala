package day10

import org.apache.spark.sql.SparkSession

object demo06_ReadJDBC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("demo05_ReadCSV")
      .getOrCreate()


    //spark.read.jdbc()
  }
}

package cn.doitedu.day10

import org.apache.spark.sql.SparkSession

/**
 * ROC文件格式也是列式存储，支持压缩，也有schema，也支持映射下推和谓词下推
 *
 */
object C04_ReadORCDemo1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C04_WriteToORCDemo1")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.orc("data/orc")

    df.printSchema()

    df.show()
    
  }

}

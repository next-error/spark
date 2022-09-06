package cn.doitedu.day10

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * spark sql读取txt、JSON、csv格式
 * 这几种文件类型，哪一种最好呢？
 *
 * txt(没有schema)
 * 1,tom,18,9999.99
 *
 * csv(有scheam)
 * id,name,age,fv
 * 1,tom,18,9999.99
 *
 * JSON(有Schema)
 * {"id": 1, "name": "tome", "age": 18, "fv":9999.99}
 *
 * 考虑两个问题：
 * 数据存储起来更加紧凑（占用空间小）
 * csv
 * 优点：占用永久较小，只有表头有字段名称
 * 缺点：schema表现形式单一，不支持数组、嵌套类型
 * json
 * 缺点：占用的空间较大（额外存储一些数据：大括号、冒号，引号，而且每一行都要有字段名称）
 * 优点：schema表现形式更加丰富，比如支持数组、嵌套类型
 *
 * -----------------------------
 * 能不能将csv和json的优点都集合起来的存储格式呢？
 * 有，Parquet和ORC，列式存储的文件格式，并且支持压缩
 * 有丰富的Schema，并且存储起来更加节省空间，读写更加高效、而且支持映射下推（按需读取），还支持谓词下推
 *
 * Parquet文件是Spark SQL的最爱
 *
 */
object C03_WriteToParquetDemo1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("C02_ReadCsvDemo2")
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .option("header", "true") //指定第一行为表头信息
      .option("inferSchema", "true") //推断字段类型
      .option("delimiter", "|") //指定字段的分隔符，默认是逗号
      .csv("data/person2.csv")

    df.printSchema()

    //将数据保存成Parquet格式
    df.write
      //.mode(SaveMode.Append)
      .mode(SaveMode.Overwrite)
      .parquet("data/parquet")

    //Thread.sleep(88888888)


  }

}

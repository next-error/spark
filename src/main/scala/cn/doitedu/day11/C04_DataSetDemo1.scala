package cn.doitedu.day11

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

/**
 * RDD 弹性的、可复原的分布式数据集，是比较底层的API
 *
 * spark 1.3 出现了DataFrame，也是分布式数据集，是RDD高级的封装，有了更多的描述信息，对列的名称、类型、可以不可以为空
 * spark 1.5 Dataset，也是分布式数据集，是RDD高级的封装
 * spark 2.0 将Dataset和 DataFrame API进行统一（智能、高效、方便）
 * spark 3.0 可以根据程序运算时的统计计算，自适应优化执行计划（更智能、更高效、更方便）
 *
 * DataFrame 是一种特殊类型的Dataset，即Dataset[Row]
 *
 * 1.Dataset和DataFrame都有额外的描述信息（schema）
 * 2.Dataset和DataFrame有执行计划（更智能、更高效的抽象的数据集）
 * 3.Dataset和DataFrame的API支持SQL、和DSL风格，高度封装的API，使用起来更方便
 * 4.Dataset和DataFrame都有自己的Encoder（编解码器，序列化的反序列化方式）
 * 5.Dataset和DataFrame是强数据类型的，在执行之前，可以根据schema信息就可以获取数据的类型
 *
 * Dataset或DataFrame = RDD + schema + Encoder + plan(执行计划)
 */
object C04_DataSetDemo1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("C04_DataSetDemo1").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("data/user.txt")
    //支持部分RDD的API，可以使用RDD的函数式是编程了

    //Dataset中有schema
    //lines.printSchema()
    val schema: StructType = lines.schema
    println(schema)

    //Dataset中持有者RDD的引用
    //Dataset底层使用的就是RDD
    val rdd: RDD[String] = lines.rdd

    //或获取执行计划，获取更多的执行计划信息
    lines.explain(true)
    val execution: QueryExecution = lines.queryExecution

    //Dataset有对应类型的Encoder（即序列化的反序列的方式）
    val encoder: Encoder[String] = lines.encoder



  }

}

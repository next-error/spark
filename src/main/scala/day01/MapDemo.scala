package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD的map方法使用
 * 对数据进行映射操作,是一个转换算子
 * 调用map不会立即对数据进行运算,而是记录对哪一个RDD调用了map方法,传入了什么函数
 */
object MapDemo {
  def main(args: Array[String]): Unit = {
    //创建SparkContext的Source方法,创建RDD(Spark上下文,用于创建原始RDD)
    val conf = new SparkConf().setMaster("local[*]").setAppName("mapDemo")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("data/words.txt")

    //2. 调用RDD的转换算子 (Transformation(s), 调用完转换算子后,生成新的RDD
    val upperLines: RDD[String] = lines.map(_.toUpperCase())

    //3. 调用行动算子,(Action,生成Tasks,然后将Task提交到集群或在本地执行)
    //upperLines.foreach()
    upperLines.saveAsTextFile("out")
    //4.释放资源
    sc.stop()
  }

}

package cn.doitedu.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD的 reduceByKey方法的使用
 *
 * reduceByKey是一个转换算子，对RDD中对应的数据进行分组聚合，只对key相同的value进行聚合
 * reduceByKey可以在每个分区中，先局部聚合再全局聚合，目的是为了减少shuffle数据传输，提高效率
 *
 * reduceByKey也是一个Transformation，会生成一个新的RDD，调用reduceByKey不会立即执行
 *
 */
object C05_ReduceByKeyDemo {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MapDemo")
    val sc = new SparkContext(conf)

    //2.调用SparkContext的Source方法，创建RDD(spark上下文，用于创建原始RDD）
    val lines: RDD[String] = sc.textFile("data/avg.txt")

    //3.调用RDD的转换算子（Transformation（s）），调用完转换算子后，会生成新的RDD
    val gradeScoreAndOne: RDD[(String, (Double, Int))] = lines.map(line => {
      val fields = line.split(",")
      val grade = fields(2)
      val score = fields(1).toDouble
      (grade, (score, 1))
    })

    //按照班级分组聚合
    val reduced: RDD[(String, (Double, Int))] = gradeScoreAndOne.reduceByKey((a, b) => {
      (a._1 + b._1, a._2 + b._2)
    })

    //    val res: RDD[(String, Double)] = reduced.map(t => {
    //      (t._1, t._2._1 / t._2._2)
    //    })
    val res = reduced.map{ case (grade, (score, count)) => {
      (grade, score / count)
    }}

    //触发Action
    res.foreach(println)


    //5.释放资源
    sc.stop()


  }

}

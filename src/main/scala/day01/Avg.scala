package day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Avg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AvgByGrade")
    val context = new SparkContext(conf)
    val lines: RDD[String] = context.textFile("data/avg.txt")
    val scoreAndGrade: RDD[(String, (Double, Int))] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val score = fields(1).toDouble
      val grade = fields(2)
      (grade,(score,1))
    })
    //按照班级分组聚合
    val sumByGrade: RDD[(String,(Double, Int))] = scoreAndGrade.reduceByKey((a , b)=>{
      (a._1+b._1,a._2+b._2)
    })

    val scoreByGrade: RDD[(String, Double)] = sumByGrade.map { case (grade, (score, count)) => {
      (grade, (score / count))
    }
    }

    scoreByGrade.foreach(println)




    context.stop()
  }

}

package cn.doitedu.day08

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark中的累计器，在job运行时，可以对数据进行统计，先在Executor进行统计每个分区的，然后再返回到Driver
 * 使用spark的累加器API，特点是可以见每个Task计算的结果收集到Driver
 *
 * 累计器的特殊用法，多次触发Action，观察累计器返回的结果
 *
 */
object C01_AccumulatorDemo3 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd1 = sc.parallelize(Array(1,2,3,4, 5,6,7,8,9), 2)

    //使用累加器，是spark一个特殊的变量（Driver定义的）
    val oddAcc: LongAccumulator = sc.longAccumulator("odd-acc")
    //Task
    val rdd2 = rdd1.map(n => {
      if (n % 2 != 0) {
        oddAcc.add(1) //闭包，函数的执行是在Executor中的Task中
      }
      n * 10
    })
    //如果多次触发Action后，希望累计器的结果计算的是正确的
    //rdd2.cache()


    //第一次触发Action
    val res1 = rdd2.collect()
    //为什么acc的结果为 0 ？
    //并没有将Executor端task计算的数据返回给Driver
    println("第一次返回acc的结果： " + oddAcc.sum)

    //第二次触发Action
    val res2 = rdd2.count()
    //为什么acc的结果为 0 ？
    //并没有将Executor端task计算的数据返回给Driver
    println("第二次返回acc的结果： " + oddAcc.sum)


    sc.stop()


  }

}

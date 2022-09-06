package cn.doitedu.day07

import java.net.InetAddress

import cn.doitedu.day06.RuleMapObjNotSer
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/***
 *
 * 演示Spark的另外一种序列化的问题
 */
object C01_SerTest {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建RDD
    val rdd1 = sc.parallelize(Array("tom,18,male", "jerry,20,female", "kitty,20,male"), 2)

    //对数据进行整理
    val rdd2 = rdd1.map(line => {
      val field = line.split(",")
      val name = field(0)
      val age = field(1).toInt
      val gender = field(2)
      new UserBean(name, age, gender)
    })

    //按照性别进行分组（会shuffle）
    //由于调用了shuffle算子，shuffleWrite时要将数据溢写到本地磁盘，而封装数据的UserBean没有实现序列化接口
    val grouped = rdd2.groupBy(_.gender)

    val res = grouped.collect()

    println(res.toBuffer)

    sc.stop()

  }
}

//一个普通的class，封装数据
//如果想要使用普通的class封装数据，就必须实现Serializable接口
class UserBean(val name: String, val age: Int, val gender: String) {

}

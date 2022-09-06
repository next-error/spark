package day07

import org.apache.spark.{SparkConf, SparkContext}

object SerTest {
  /**
   * Spark 另一个序列化问题
   *
   */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("tom,18,male", "jerry,20,female", "kitty,20,male"), 2)
    val rdd2 = rdd1.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val gender = fields(2)
      /*val tuple = (name, age, gender)
      tuple*/
      new UserBean(name, age, gender)

    })
    //按照性别分组会shuffle,
    //由于调用了shuffle算子，shuffleWrite时要将数据溢写到本地磁盘，而封装数据的UserBean没有实现序列化接口
    //封装数据建议使用case class
    val grouped = rdd2.groupBy(_.gender)

    val res = grouped.collect()
    println(res.toBuffer)
    sc.stop()
  }
}

 class UserBean (val name: String,val age :Int, val gender:String){

}
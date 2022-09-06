package cn.doitedu.day12

import org.apache.spark.sql.SparkSession

/**
 * sparkSQL兼容hive，使用hive的源数据库，可以使用hive的sql
 * 并且可以编程
 *
 * 1.将hive-stie.xml翻入到resource目录
 * 2.在pom文件添加依赖
 * <dependency>
 * <groupId>org.apache.spark</groupId>
 * <artifactId>spark-hive_2.12</artifactId>
 * <version>3.2.1</version>
 * </dependency>
 * 3.在创建sparksession时， .enableHiveSupport()
 *
 */
object EnableHiveSupportDemo {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport() //开启对hive的支持，把hive-site.xml放入到当前项目的resource目录下
      .getOrCreate()

    //建表语句、load data、删除表
    //生成DataFrame（从Mysql表里面来的）
    val df = spark.sql("select gender, max(age) from tb_users group by gender")

    df.show()

    spark.stop()

  }
}

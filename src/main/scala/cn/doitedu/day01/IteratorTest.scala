package cn.doitedu.day01

import scala.io.Source

object IteratorTest {

  def main(args: Array[String]): Unit = {

    val source = Source.fromFile("data/words.txt")

    //读取数据
    val lines: Iterator[String] = source.getLines()

    val words: Iterator[String] = lines.flatMap(line => {
      val fields = line.split(" ")
      println("flatMap ~~~~~~~~~~")
      fields
    })

    val filtered = words.filter(w => {
      println("filter !!!!!")
      !w.startsWith("h")
    })

    filtered.foreach(w => {

      println(w)

    })

    source.close()
  }

}

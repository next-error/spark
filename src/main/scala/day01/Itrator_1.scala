package day01

import scala.io.{BufferedSource, Source}

object Itrator_1 {
  def main(args: Array[String]): Unit = {
    val source: BufferedSource = Source.fromFile("data/words.txt")
    val lines: Iterator[String] = source.getLines()
    val words = lines.flatMap(_.split(" "))
    val filtered: Iterator[String] = words.filter(w => {
      !w.startsWith("h")
    })



    source.close()
  }
}

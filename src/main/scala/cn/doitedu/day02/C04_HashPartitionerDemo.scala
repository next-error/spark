package cn.doitedu.day02

/** *
 * 演示spark的默认分区器，HashPartitioner
 */
object C04_HashPartitionerDemo {

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def main(args: Array[String]): Unit = {
    val key = "kafka"
    val index = nonNegativeMod(key.hashCode, 4)
    println(index)
  }
}

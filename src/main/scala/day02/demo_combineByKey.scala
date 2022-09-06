package day02

object demo_combineByKey {
  /**
   * 所有ByKey算子的底层
   *combineByKey:传入三个函数,
   * 第一个函数,在上游,先进行分组,将key第一次出现的value进行处理
   * 第二个函数,在上游,将相同的key,再次出现的value进行处理
   * 第三个函数:在下游,将每个分区局部聚合的结果在下游进行全局聚合
   *
   * 特殊情况:
   * combineByKeyWithClassTag实现聚合,当第四个参数(mapSideCombine默认为true)为false时,三个函数只会执行前两个,且是在下游执行
   */
}

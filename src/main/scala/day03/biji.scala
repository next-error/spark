package day03

object biji {
  /**
   * reduceByKey:传入的函数,上游下游计算逻辑一样
   * distinct(),要传入分区数量,不传默认和上游相同
   * Aggreator()传入三个函数,上游下游聚合函数可以不同
   * rdd1.cogroup()传入rdd,最多传入三个,要求rdd数据必须是对偶元组,而且key的类型必须相同,key相同的会进到同一个分区
   * rdd1.intersection() 求交集
   * subtract() 求差集 底层调用subtractByKey,再底层new SubtractRDD
   * shuffle需要是kv类型数据
   * join,默认inner join,join的两个rdd必须都存在的key才能join(笛卡尔积)
   * sortBy,会产生job,用于采样构建分区规则(确定分区范围),但不是Action算子
   *
   * Action算子:
   * count:先每个task在其分区局部计数,返回Driver端再进行全局计数
   * sum:
   * aggregate:传入两个函数,对rdd数据进行聚合,局部聚合和全局聚合的逻辑可以不一样
   * take
   * frist
   * min
   * max
   * top:先局部,再全局
   *
   * tolist:会把数据放入内存中
   */

}

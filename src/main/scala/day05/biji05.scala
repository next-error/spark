package day05

/**
 * 序列化和线程安全
 *算子在Driver端调用,传入rdd的函数在Executor里执行
 * 一个DVG生成一个job
 * 有shuffle会切分stage
 *
 *
 * 函数内部使用了外部的变量,需要序列化;
 * 使用class还是object实现序列化接口,要看不同的分区是否有公共变量
 *
 *
 */
class biji05 {

}

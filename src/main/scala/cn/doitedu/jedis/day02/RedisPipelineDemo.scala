package cn.doitedu.jedis.day02

import redis.clients.jedis.{Jedis, Pipeline, Transaction}

/**
 * 演示的是Redis的Pipeline（Redis的事务），只有单机版支持事务
 */
object RedisPipelineDemo {

  def main(args: Array[String]): Unit = {

    val jedis = JedisConnectionPool.getJedis()
    var transaction: Transaction = null
    try {
      //开启事务,multi方法返回的是带事务的jedis连接
      transaction = jedis.multi()
      transaction.set("aaaaa", "11111")
      //val i = 1 / 0
      transaction.set("bbbbb", "22222")

      //提交事务
      transaction.exec()
    } catch {
      case e: Exception => {
        //放弃事务
        transaction.discard()
      }
    } finally {
      transaction.clear()
      jedis.close()
    }


  }

}

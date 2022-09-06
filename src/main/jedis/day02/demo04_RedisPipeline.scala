package day02

import redis.clients.jedis.Transaction
import untils.JedisConnectionPool

/** |
 * Redis的Pipeline (Redis的事务) ,只有单机版支持事务
 */
object demo04_RedisPipeline {
  def main(args: Array[String]): Unit = {
    val jedis = JedisConnectionPool.getJedis()
    jedis.select(1)
    jedis.flushDB()

    //开启事务
    var transaction: Transaction = null
    try {
      transaction = jedis.multi()
      transaction.set("aaa", "1111")
      transaction.set("bbb", "2222")
      //提交事务
       transaction.exec()
    } catch {
      case e: Exception => {
        transaction.discard()
      }
    } finally {
      jedis.close()
    }


  }
}

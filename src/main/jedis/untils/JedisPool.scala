package untils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * 连接池在一个进程中有一个即可,所以单例即可
 */
object JedisConnectionPool {
  //在object中的代码相当于java中的静态代码块中
  private val config = new JedisPoolConfig()
  config.setMaxTotal(10) //最大连接数
  config.setMinIdle(2) //最小活跃连接
  config.setMaxIdle(5) //最大活跃连接
  config.setTestOnBorrow(true) //在使用连接时,会对连接进行检测
  private val jedisPool = new JedisPool(config, "linux01", 6379)

  def getJedis(): Jedis = {
    jedisPool.getResource
  }


}

package cn.doitedu.jedis.day02

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * 连接池，在一个进程中，有一个即可，所以连接池是单例的即可
 *
 * object本身就是单例的
 *
 */
object JedisConnectionPool {

  //写在object中的代码，相当于写在java总的静态代码块中的代码

  private val config = new JedisPoolConfig()
  config.setMaxTotal(10) //连接池最大连接数量
  config.setMinIdle(2) //最小活跃连接
  config.setMaxIdle(5) //最大活跃连接
  config.setTestOnBorrow(true) //在使用连接时，会对连接进行检测
  private val jedisPool = new JedisPool(config, "node-1.51doit.cn", 6379, 5000, "123456")

  def getJedis(): Jedis = {
    jedisPool.getResource
  }



}

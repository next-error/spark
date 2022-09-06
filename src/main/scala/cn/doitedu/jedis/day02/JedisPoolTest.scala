package cn.doitedu.jedis.day02

object JedisPoolTest {

  def main(args: Array[String]): Unit = {

    val jedis = JedisConnectionPool.getJedis()

    jedis.select(1)

    jedis.set("aaa", "111")

    //如果使用的是连接池，就将连接还回去，如果使用的普通的jedis连接，就关闭
    jedis.close()




  }

}

package cn.doitedu.jedis.day02;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

public class JedisTest {

    public static void main(String[] args) {

        Jedis jedis = new Jedis("node-1.51doit.cn", 6379);
        jedis.auth("123456");
        Transaction transaction = null;
        try {
            transaction = jedis.multi();
            //Pipeline pipelined = jedis.pipelined();
            transaction.set("aa", "111");
            //int i = 10 / 0;
            transaction.set("bb", "111");
            //pipelined.sync();
            transaction.exec();
        } catch (Exception e) {
            e.printStackTrace();
            transaction.discard();
        }

    }
}

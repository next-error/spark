package cn.doitedu.day03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


public class ConnectionUtils {

    public static Connection getConnection() throws Exception{

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node-1.51doit.cn:2181,node-2.51doit.cn:2181,node-3.51doit.cn:2181");
        // 创建一个hbase的客户端连接
        return ConnectionFactory.createConnection(conf);
    }
}

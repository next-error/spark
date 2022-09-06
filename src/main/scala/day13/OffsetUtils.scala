package day13

import java.sql.DriverManager

import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

object OffsetUtils {

  //查询MySQL，返回最新的历史偏移量
  def queryHistoryOffsetFromMySQL(appName: String, groupId: String): Map[TopicPartition, Long] = {

    val historyOffsets = new mutable.HashMap[TopicPartition, Long]()
    //查询MySQL的tb_history_offset表
    val conn = DriverManager.getConnection("jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8", "root", "123456")
    val pstm = conn.prepareStatement("select topic_partition, `offset` from tb_history_offset where app_gid = ?")
    //设置查询条件
    pstm.setString(1, appName + "_" + groupId)
    //执行查询
    val rs = pstm.executeQuery()
    while (rs.next()) {
      //读取处理topic和partition
      val topic_partition = rs.getString(1)
      val fields = topic_partition.split("_")
      val topic = fields(0)
      val partition = fields(1).toInt
      //读取出偏移量
      val offset = rs.getLong(2)
      historyOffsets.put(new TopicPartition(topic, partition), offset)
    }
    if(rs != null) rs.close()
    if(pstm != null) pstm.close()
    if(conn != null) conn.close()

    historyOffsets.toMap
  }



}

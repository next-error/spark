package cn.doitedu.day06

//使用一个没有序列化的class，里面用map保存维度数据
class RuleMapClassSer extends Serializable {

  val rules = Map(
    ("ln", "辽宁省"),
    ("sd", "山东省"),
    ("sh", "上海市"),
    ("hn", "河南省")
  )

}

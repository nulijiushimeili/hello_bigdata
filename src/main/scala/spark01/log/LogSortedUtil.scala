package spark01.log

/**
  * create by nulijiushimeili on 2018-08-06
  */
object LogSortedUtil {

  /**
    * 自定义一个二元组比较器
    */
  object TupleOrdering extends scala.math.Ordering[(String, Int)] {
    override def compare(x: (String, Int), y: (String, Int)): Int =
    // 按照出现的次数进行比较,也就是按照二元组的第二个元素进行比较
      x._2.compare(y._2)
  }

}

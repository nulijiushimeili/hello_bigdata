package spark01.core

import org.apache.spark.{SparkConf, SparkContext}
/**
  * 使用Ordered & Serializable 对 RDD中的数据排序
  * 也可以对DataFrame的数据进行排序,但是仍然是使用RDD实现
  *
  */
class SecondSort(val first:Int,val second:Int) extends Ordered[SecondSort] with Serializable {
  override def compare(that: SecondSort): Int = {
    if(this.first - that.first != 0){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}


object SecondSort{
  def main(args:Array[String]): Unit ={
    val sc = new SparkContext(new SparkConf().setAppName("SecondSort").setMaster("local"))
    val path = "file:\\D:\\mycode1\\program\\spark\\spark01\\data\\second_sort.txt"

    val line = sc.textFile(path).flatMap(_.split("\n"))
    val res = line.map(x => (new SecondSort(x(0).toInt,x(1).toInt),x)).sortByKey().map(_._2)
    res.foreach(println)

    sc.stop()
  }
}
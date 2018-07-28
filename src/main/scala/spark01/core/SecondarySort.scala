package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  *     二次排序
  *
  */
class SecondarySort(val first :Int,val second:Int)extends Ordered[SecondarySort]with Serializable{
  override def compare(that: SecondarySort): Int = {
    if(this.first != that.first){
      this.first - that.first
    }else{
      this.second - that.second
    }
  }
}

object SecondarySort {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("SecondarySort"))
    val file = sc.textFile("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\second_sort.txt")
    val lines = file.flatMap(_.split("\n"))
    val sortedRes = lines.map(x => (new SecondarySort(x.split(",")(0).toInt,x.split(",")(1).toInt),x)).sortByKey().map(x=>x._2)
    sortedRes.foreach(println)

    sc.stop()

  }

}

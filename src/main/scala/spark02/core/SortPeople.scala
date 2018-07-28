package spark02.core

/**
  * 二次排序,有线程安全问题
  * 排序结果如果是随机的,一般考虑线程安全问题
  *
  */

import org.apache.spark.{SparkConf, SparkContext}

//class SortPeople(val name: String, val age: Int)
class SortPeople(val name: String, val age: Int)
  extends Ordered[SortPeople] with Serializable {
  override def compare(that: SortPeople): Int = {
    if (this.name.compareTo(that.name)>0) {
      1
    } else if(this.name.compareTo(that.name)<0){
      -1
    }else {
      this.age - that.age
    }
//    if(this.name!=that.name){
//      this.name-that.name
//    }else{
//      this.age-that.age
//    }
  }
}

object SortPeople {
  def main(args: Array[String]): Unit = {
//    val spark = SparkSession
//      .builder()
//      .appName("sortPeople")
//      .master("local[4]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val lines = spark.sparkContext
//      .textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\people.txt")
//      .flatMap(_.split("\n"))
    val sc = new SparkContext(new SparkConf().setAppName("sortPeople").setMaster("local"))
    val lines = sc.textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\people.txt")
//    val lines = sc.textFile(
//      "D:\\mycode1\\program\\spark\\sparksql\\file\\secondarySort.txt")
    val mapRDD = lines.map(x =>
      (new SortPeople(x.split(",")(0), x.split(",")(1).toInt), x))

    val sortRes = mapRDD.sortByKey().map(x => x._2)
    sortRes.foreach(println)

  }
}
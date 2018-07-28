package spark02.core

import org.apache.spark.{SparkConf, SparkContext}

object TestTextFile {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("wordcont").setMaster("local[*]"))
    val file = sc.textFile(
      "D:\\mycode1\\program\\spark\\sparksql\\src\\main\\resources\\core-site.xml")
    val wc = file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wc.foreach(println)

    val list = List(0,1,3,1,4,5,5,6,7,2,3,4,5,6,7,8,8,9)
    val listRdd = sc.parallelize(list)
    val itemCount = listRdd.map((_,1)).reduceByKey(_+_)
    itemCount.foreach(println)
    println("-------------------------------------------")
    val getFilter = listRdd.filter(x => x>3 && x<6)
    getFilter.foreach(println)

  }
}

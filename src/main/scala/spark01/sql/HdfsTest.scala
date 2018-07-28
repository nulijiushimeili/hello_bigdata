package spark01.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object HdfsTest {
  def main(args: Array[String]): Unit = {
//    if(args.length<1){
//      System.err.println("Usage: HdfsTest <file>")
//      System.exit(1)
//    }

    val spark = SparkSession
      .builder()
      .appName("HDFS test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

//    val file = spark.read.text(args(0)).rdd
    val filePath = "file:\\D:\\mycode1\\program\\spark\\spark01\\data\\emp.txt"
    // Get some row rdds.
    val file: RDD[Row] = spark.read.text(filePath).rdd

//    [1 xiaoming worker xiaoli 20000101 5000 500 1]
//    [2 xiaohuan worker xiaoli 20000605 6000 500 1]
//    [3 xiaoqian worker xiaoli 20010102 5000 200 1]
//    [4 xiaoquan leader xiaoli 20000105 5000 600 1]
//    [5 benboba worker xiaosun 20020504 5000 500 2]
//    [6 baboben worker xiaosun 20001002 6000 200 2]
//    [7 jilinggui leader xiaoli 20030512 9000 800 2]

    val mapped = file.map(_.length).cache()

    for(iter <- 1 to 10){
      val start = System.currentTimeMillis()
      for(x <- mapped) {x+2}
      val end = System.currentTimeMillis()
      println(s"Iteration $iter took ${end - start} ms")
    }

    file.foreach(println)

    spark.stop()
  }
}

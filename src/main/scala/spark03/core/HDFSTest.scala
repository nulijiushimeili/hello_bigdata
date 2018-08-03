package spark03.core

import org.apache.spark.sql.SparkSession
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-03
  */
object HDFSTest {
  def main(args: Array[String]): Unit = {
    if (args .length <1){
      System.err.println("Usage : HDFSTest <file>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("HDFS test")
      .master("local[*]")
      .config(SparkProperties.warehouse,SparkProperties.warehouseDir())
      .getOrCreate()

    val file = spark.read.text(args(0)).rdd
    val mapped = file .map(s => s.length).cache()

    for (iter <- 1 to 10){
      val start = System.currentTimeMillis()
      for (x <- mapped ) x + 2
      val end = System.currentTimeMillis()
      println("Iteration " + iter  + " took " + (end - start) + " ms.")
    }

    spark.stop()
  }
}

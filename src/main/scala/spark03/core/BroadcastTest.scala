package spark03.core

import org.apache.spark.sql.SparkSession
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-03
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val blockSize = if (args.length > 2) args(2) else "4096"

    val spark = SparkSession.builder()
      .appName("broadcast test")
      .master("local[*]")
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    val sc = spark.sparkContext

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===================")
      val startTime = System.nanoTime()
      val barr1 = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to 10, slices)
        .map {
          _ => barr1.value.length
        }
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime() - startTime) / 1E6))
    }

    spark.stop()
  }
}
package spark03.core

import org.apache.spark.sql.SparkSession
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-03
  */
object MultiBroadcastTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    //    val slices = if(args.length>0) args(0).toInt else 2
    //    val num = if (args.length>1) args(1).toInt else 1000000

    val slices = 2
    val num = 1000000
    val arr1 = new Array[Int](num)
    for (i <- arr1.indices) {
      arr1(i) = i
    }

    val arr2 = new Array[Int](num)
    for (i <- arr2.indices) {
      arr2(i) = i
    }

    val barr1 = spark.sparkContext.broadcast(arr1)
    val barr2 = spark.sparkContext.broadcast(arr2)

    val observedSizes = spark.sparkContext
      .parallelize(1 to 10, slices)
      .map(_ => (barr1.value.length, barr2.value.length))

    observedSizes.collect().foreach(i => println(i))

    spark.stop()

  }
}

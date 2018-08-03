package spark03.core

import org.apache.spark.sql.SparkSession
import spark03.sql.SparkProperties

import scala.util.Random

/**
  * create by nulijiushimeili on 2018-08-03
  */
object GroupByTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

//    val numMappers = if (args.length > 0) args(0).toInt else 2
//    val numKVPairs = if (args.length > 1) args(1) .toInt else 1000
//    val valSizes = if (args.length > 2) args(2) .toInt else 1000
//    val numReducers = if(args.length > 3) args(3).toInt else numMappers

    val numMappers = 2
    val numKVPairs = 1000
    val valSizes = 1000
    val numReducers = numMappers

    val pairs1 = spark.sparkContext
      .parallelize(0 until numMappers, numMappers)
      .flatMap{ p =>
        val ranGen = new Random()
        val arr1 = new Array[(Int,Array[Byte])](numKVPairs)
        for (i <- 0 until numKVPairs){
          val byteArr = new Array[Byte] (valSizes)
          ranGen.nextBytes(byteArr)
          arr1(i) = (ranGen.nextInt(Int.MaxValue),byteArr)
        }
        arr1
      }.cache()

    pairs1.count()

    println(pairs1.groupByKey(numReducers).count())

    spark.stop()

  }
}

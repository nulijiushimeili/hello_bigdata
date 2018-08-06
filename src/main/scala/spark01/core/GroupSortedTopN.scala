package spark01.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * create by nulijiushimeili on 2018-08-05
  */
object GroupSortedTopN {
  val random: Random.type = Random

  def main(args: Array[String]): Unit = {
    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\groupsort.txt"
    val K = 3

    // 1. Create SparkContext
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)

    // 2. Create RDD
    val rdd = sc.textFile(path)

    // 3. Operation RDD
    val resRDD = rdd.map(_.split(" "))
      .filter(arr => arr.length == 2)
      .map(arr => (arr(0), arr(1).toInt))
      .groupByKey()
      .flatMap {
        case (item1, iter) => {
          // Sorted iter and get max value in forword.
          val topKItem2 = iter.toList.sorted.takeRight(K)
          // Return the result
          topKItem2.map(item2 => (item1, item2))
        }
      }

    // cache
    resRDD.cache()

    // 4. Result output
    resRDD.foreachPartition(iter => {
      // Output result using this api is best choose.
      iter.foreach(println)
    })

    // Save the result.
    resRDD.saveAsTextFile(s"file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\${System.currentTimeMillis()}")

    resRDD.unpersist()

    /** 优化一: 使用两阶段聚合 ==> 解决数据倾斜 */
    val resRDD2 = rdd.map(_.split(" "))
      .filter(arr => arr.length == 2)
      .map(arr => ((random.nextInt(100), arr(0)), arr(1).toInt))
      .groupByKey()
      .flatMap {
        case ((_, item1), iter) => {
          // 对iter进行排序然后获得数值最大的前K个数据
          val topKItem2 = iter.toList.sorted.takeRight(K)
          // 返回结果, 去掉随机数
          topKItem2.map((item1, _))
        }
      }
    resRDD2.foreachPartition(println)

    /** 优化方式二: 使用aggregateByKey 替代 groupByKey ==> 主要解决性能问题,额外解决OOM异常 */
    val resRDD3 = rdd.map(_.split(" "))
      .filter(_.length == 2)
      .map(arr => (arr(0), arr(1).toInt))
      .aggregateByKey(ArrayBuffer[Int]())(
        (u, v) => {
          // 将u 和 v 合并, 然后返回一个新的u对象 ==> 将v添加到集合u中,然后排序获取前k个
          u += v
          u.sorted.takeRight(K)
        },
        (u1, u2) => {
          u1 ++= u2
          u1.sorted.takeRight(K)
        }
      )

    resRDD3.foreachPartition(println)
  }

}

package spark01.core

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PairRDDTest {
  def main(args: Array[String]): Unit = {
    test4()

  }

  def test1(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("PairRDD")
    val sc = new SparkContext(conf)

    val arr = Array(1, 3, 4, 5, 6, 7, 5, 78)
    val rdd = sc.parallelize(arr).map((_, 1))

    rdd.foreach(println)

    sc.stop()
  }

  def test2(): Unit = {
    val spark = SparkSession.builder()
      .appName("PairRDD")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val rdd = sc.parallelize(List(0 to 15).flatten).map((_, 1))

    rdd.foreach(println)

    sc.stop()
    spark.stop()

  }

  def test3(): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("pairRDD")
    val sc = new SparkContext(conf)


//    val rdd = sc.parallelize(List(1 to 23).flatten)
    val rdd = sc.parallelize(List(1,2,4,54,65,6,7,7))

    val pairRDD = rdd.map ( x =>
      if (x % 2 == 0) (x,x+1) else {
        (x, x+1)
      }
    ).persist(StorageLevel.MEMORY_ONLY)


    val mapRDD = pairRDD.map(i=>(i._1,i._2+2))

    pairRDD.filter{case (x,y)=> x>4}

    val mapRDD2 = pairRDD.mapValues(i => (i,1)).reduceByKey((x,y)=>(x._1+y._1,x._2*10))


    mapRDD.foreach(println)
    mapRDD2.foreach(println)

    sc.stop()
  }

  def test4(): Unit ={
    val conf = new SparkConf().setAppName("PairRDD join").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val table1 = sc.parallelize(List(("k1",1),("k2",2),("k3",3),("k1",5)))
    val table2 = sc.parallelize(List(("k0",10),("k2",20),("k3",30)))

    table1.join(table2).foreach(println)

    println("-------------------------------")

    table1.leftOuterJoin(table2).foreach(println)

    println("-------------------------------")

    table1.rightOuterJoin(table2).foreach(println)

    println("-------------------------------")

    table1.groupByKey().foreach(println)

    println("-------------------------------")

    table1.reduceByKey(_+_).foreach(println)

    println("-------------------------------")

    table1.cogroup(table2).cogroup(table2).cogroup(table1).foreach(println)

    sc.stop()
  }

}

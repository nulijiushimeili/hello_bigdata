package spark03.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by nulijiushimeili on 2018-07-27
  */
object JoinRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    testJoin(sc)
    sc.stop
  }

  /**
    * test pair rdd join operation.
    *
    * @param sc SparkContext
    */
  def testJoin(sc: SparkContext): Unit = {
    val table1 = sc.parallelize(List(("k1", 10), ("k2", 20), ("k2", 15), ("k3", 15))) // pairRDD里的Tuple2
    val table2 = sc.parallelize(List(("k1", 11), ("k2", 12), ("k1", 100)))

    table1.join(table2).foreach(println)
    println("----------------------------------")

    table1.leftOuterJoin(table2).foreach(println)
    println("------------------------------------")

    table1.rightOuterJoin(table1).foreach(println)
    println("------------------------------------")

    table1.groupByKey().foreach(println)
    println("------------------------------------")

    table1.reduceByKey(_ + _).foreach(println)
    println("------------------------------------")

    /**
      * 其实就是多个partition在进行cogroup时，会产生一个Array[K,T] 这样的一个对象，
      * K为每partition中每个个元组的K，T中有多个Iterable,
      * 每个Iterable中封装是该Iterable对应的唯一partition中的相同K的元组对应的V。
      * 每一个Iterable对应唯一的Partition
      */
    val cogroupRes = table1.cogroup(table2).cogroup(table2).cogroup(table2)

    cogroupRes.foreach(println)
  }


}

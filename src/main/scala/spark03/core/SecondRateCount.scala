package spark03.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by nulijiushimeili on 2018-07-28
  */
object SecondRateCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val fileRDD= sc.textFile(args(0))
    val fileRDD= sc.textFile("file:\\D:\\tmp\\data\\2015082818")
      .filter(line => line.trim.length>0)
      .map{line =>
        val arr = line.split("\t")
        val date = arr(17).substring(0,10)
        val sessionId = arr(10)
        val url = arr(1)
        (date, sessionId,url)
      }.filter(i=>i._3.length>0).persist(StorageLevel.DISK_ONLY)

    val rdd2 = fileRDD.map(line => (line._1+" " + line._2,1)).reduceByKey(_+_)
      .map{ i =>
        val arr = i._1.split("_")
        val date = arr(0)
        val sessionId = if(arr(1).length>0)1 else 0
        val second = if(i._2>= 2)1 else 0
        (date,(sessionId,second))
      }.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))


      rdd2.collect().take(10).foreach(println)

    sc.stop()

  }
}

package spark03.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * create by nulijiushimeili on 2018-07-29
  */
object PartitionVisitCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val filePath = "file:\\D:\\tmp\\data\\2015101018"
    val fileRDD= sc.textFile(filePath).filter(line => line.trim.length>0)
      .map{line =>
        val arr = line .split("\t")
        val date = arr(17).substring(0,10)
        val guid = arr(5)
        val url = arr(1)
        val uid = arr(18)
        (uid,(guid,url))
      }.partitionBy(new HashPartitioner(10)) // 采用hashcode的分片方式,分成10个分区
      .persist(StorageLevel.DISK_ONLY)

    // userid
    val rdd = sc . parallelize(List(1,2,3,4,5,6,7)).map(i=>(i.toString,i+"name"))
    fileRDD.join(rdd).foreach(println)

    sc.stop()
  }
}

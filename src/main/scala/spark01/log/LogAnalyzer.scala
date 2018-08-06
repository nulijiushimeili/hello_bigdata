package spark01.log

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark03.sql.SparkProperties

/**
  * Apache log analyzer.
  *
  * create by nulijiushimeili on 2018-08-06
  */
object LogAnalyzer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster(SparkProperties.master)
    val sc = new SparkContext(conf)

    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\access.log"
    val rdd = sc.textFile(path)
    val apacheAccessLog: RDD[ApacheAccessLog] = rdd
      // filter data
      .filter(ApacheAccessLog.isValidateLogLine)
      // transform data
      .map(ApacheAccessLog.parseLogLine)
    apacheAccessLog.cache()

    /**
      * request 1: get max,min,avg content size of responses returned from the server.
      */
    val contentSizeRDD = apacheAccessLog.map(_.contentSize)
    contentSizeRDD.cache()
    val totalContentSize = contentSizeRDD.sum()
    val totalCount = contentSizeRDD.count()
    val avgSize = 1.0 * totalContentSize / totalCount
    val minSize = contentSizeRDD.min()
    val maxSize = contentSizeRDD.max()
    contentSizeRDD.unpersist()

    println(s"ContentSize Avg：$avgSize, Min: $minSize, Max: $maxSize")

    /**
      * request 2: A count of response code's returned.  ----word count application
      */
    val responseCodeResultRDD = apacheAccessLog
      // 提取需要的字段转化成key-value键值对,方便进行reduceByKey操作
      .map(resp => (resp.responseCode, 1))
      .reduceByKey(_ + _)

    println(s"""ResponseCode :${responseCodeResultRDD.collect().mkString(",")}""")

    /**
      * request 3: All IPAddresses that have accessed this server more than N times.
      * 黑名单ip地址不统计
      */
    val blackIP = Array("200-55-104-193.dsl.prima.net.ar", "10.0.0.153", "208-38-57-205.ip.cal.radiant.net")
    val broadcastIP = sc.broadcast(blackIP)
    val N = 10
    val ipAddressRDD = apacheAccessLog
      .filter(log => !broadcastIP.value.contains(log.ipAddress))
      .map(log => (log.ipAddress, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > N)

    println(s"""IP address : ${ipAddressRDD.collect().mkString(",")}""")

    /**
      * request 4: The top endpoints requested by count. ---- Top N application
      *  1. 先计算出每个endpoint的出现次数
      *  2. 再进行topK的一个获取操作，获取出现次数最多的前K个值
      */
    val K = 10
    val topKValues = apacheAccessLog
      .map(log => (log.endpoint, 1))
      .reduceByKey(_ + _)
      // 获取前10个数据,按照自定义的类排序
      .top(K)(LogSortedUtil.TupleOrdering)

    println(s"""TopK values:${topKValues.mkString(",")}""")

    apacheAccessLog.unpersist()

    sc.stop()

  }
}

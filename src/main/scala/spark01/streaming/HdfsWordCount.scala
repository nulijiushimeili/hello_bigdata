package spark01.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HdfsWordCount {
  def main(args: Array[String]): Unit = {
//    if (args.length < 1){
//      System.err.println("Usage: HdfsWordCount <directory>")
//      System.exit(1)
//    }

    // test
//    val path = "hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wc.input"
    val path = "file:\\D:\\mycode1\\program\\spark\\spark01\\data\\wc.input"

    val conf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(2))

//    val lines = ssc.textFileStream(args(0))

    // 具有tail功能,跟踪文件的变化
    val lines = ssc.textFileStream(path)
    val wc = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    wc.print()


    ssc.start()
    ssc.awaitTermination()
  }
}

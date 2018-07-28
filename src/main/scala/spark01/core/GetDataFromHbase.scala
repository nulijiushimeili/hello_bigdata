package spark01.core

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.{SparkConf, SparkContext}

object GetDataFromHbase {
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    val sc = new SparkContext(new SparkConf()
    .setAppName("get_data_from_hbase")
    .setMaster("hdfs://bigdata-senior01.ibeifeng.com:60010/hbase")
    )
    conf.set(TableInputFormat.INPUT_TABLE, "student")
    val stuRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )
    val count = stuRDD.count()
    println("student RDD count:" + count)
    stuRDD.cache()

    sc.stop()
  }
}

package spark01.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-05
  */
object MoveFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(SparkProperties.master)
      .setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)

    val file: RDD[String] = sc.textFile("file:\\D:\\Google_download\\document\\LearnMongoDB\\Lesson01")
        .repartition(1)

    file.saveAsTextFile("file:\\D:\\tmp\\mongodb")

  }
}

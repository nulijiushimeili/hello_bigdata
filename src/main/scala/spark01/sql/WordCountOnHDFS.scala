package spark01.sql

import java.io.File

import org.apache.spark.sql.SparkSession

object WordCountOnHDFS {
  private var inputPath = ""
  private var outputPath = ""

  private def prompt(): Unit ={
    println(
      """
        |You must input two path for this program:
        |One is input file name,
        |Another is output path.
      """.stripMargin
    )
  }

  private def parsePath(path:Array[String]): Unit ={
    if(path.length != 2){
      println("Need two path, more info see next:")
      prompt()
      System.exit(1)
    }

    val file = new File(path(0))
    if(!file.isFile){
      println(s"${path(0)} is not a file.")
      prompt()
      System.exit(1)
    }

    val file2 = new File(path(1))
    if(!file2.isDirectory){
        println(s"${path(1)} is not a directory!")
      prompt()
      System.exit(1)
    }

    if(file2.exists){
      println(s"${path(1)} is exists, please input a new output path.")
      prompt()
      System.exit(1)
    }

    inputPath = path(0)
    outputPath = path(1)
  }

  def main(args: Array[String]): Unit = {
//    val path = Array("hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wc.input","hdfs://bigdata-senior02.ibeifeng.com:8020/wc/out")

//    parsePath(path)

    // The path in next line is for test on windows pallet.
    inputPath = "hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wc.input"
    outputPath = "hdfs://bigdata-senior02.ibeifeng.com:8020/wc/out"

    val spark = SparkSession.builder()
      .appName("Word count on hdfs")
      .master("local[*]")
      .getOrCreate()

    println("Creating SparkContext ...")
    val sc = spark.sparkContext

    val wc = sc.textFile(inputPath)
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)

    wc.saveAsTextFile(outputPath)

    sc.stop()
    spark.stop()
  }
}

package spark01.sql

import java.io.File

import scala.io.Source._

import org.apache.spark.sql.SparkSession

object DFSReadWriteTest {
  private var localFilePath = new File(".")
  private var dfsDirPath = ""
  private val NPARAMS = 2

  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

  private def printUsage() {
    val usage =
      """DFS Read-Write Test
        |Usage:localFile dfsDir
        |localFile - (string) local file to use in test
        |dfsDir - (string) DFS directory for read/write tests""".stripMargin

    println(usage)
  }

  private def parseArgs(path: Array[String]): Unit = {
    if (path.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    var i = 0

    localFilePath = new File(path(i))

    if (!localFilePath.exists) {
      System.err.println(s"Given path ${path(i)} does not exist")
      printUsage()
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println(s"Given path ${path(i)} is not a file")
      printUsage()
      System.exit(1)
    }

    i += 1
    dfsDirPath = path(1)
  }

  def runLocalWordCount(fileContents: List[String]): Unit = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }

  def main(args: Array[String]): Unit = {
    //    val path = Array("hdfs://bigdata-senior02.ibeifeng.com:8020/wc/wc.input","hdfs://bigdata-senior02.ibeifeng.com:8020/wc/out")

    // This variable is just for test! if you want to run this project on other system,
    // you can change it, like this:
    //    val path = args
    val path = Array("D:\\mycode1\\program\\spark\\spark01\\data\\wc.input", "D:\\mycode1\\program\\spark\\spark01\\data\\out")

    parseArgs(path)

    println("Performing local word count")
    val fileContents = readFile(localFilePath.toString)
    val localWordCount = runLocalWordCount(fileContents)

    println("Creating SparkSession")
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DFS read and write test.")
      .getOrCreate()

    println("Writing local file to DFS.")
    val dfsFileName = s"$dfsDirPath/dfs_read_write_test"
    val fileRDD = spark.sparkContext.parallelize(fileContents)
    fileRDD.saveAsTextFile(dfsFileName)

    println("Reading file from DFS and running Word Count.")
    val readFileRDD = spark.sparkContext.textFile(dfsFileName)

    val dfsWordCount = readFileRDD
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .countByKey()
      .values
      .sum

    spark.stop()

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count $localWordCount and" +
        s" DFS Word Count $dfsWordCount agree.")
    } else {
      println(s"Failure! Local Word Count $localWordCount " +
        s"and DFS Word Count $dfsWordCount disagree.")
    }
  }
}
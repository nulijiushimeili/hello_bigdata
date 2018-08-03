package spark03.core

import java.io.File

import scala.io.Source._
import org.apache.spark.sql.SparkSession
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-03
  *
  * Simple test for reading and writing to a distributed file system.
  * This example does the following:
  *     1. Reads local file
  *     2. Computes word count on local file.
  *     3. Writes local file to a DFS.
  *     4. Reads the file back from the DFS.
  *     5. Computes word count on the file using Spark.
  *     6. Computes the word count results.
  */
object DFSReadWriteTest {
  private var localFilePath: File = new File(".")
  private var dfsDirPath: String = ""

  private val NPARAMS = 2

  private def readFile(fileName: String): List[String] = {
    val lineIter = fromFile(fileName).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

  private def printUsage(): Unit = {
    val usage: String = "DFS read-write Test\n" +
      "\n" +
      "Usage : localFile dfsDir \n" +
      "\n" +
      "localFile - (String) local file to use in test \n" +
      "dfsDir - (string)DFS directory for read/write tests\n"
    println(usage)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    var i = 0

    localFilePath = new File(args(i))
    if (!localFilePath.exists()) {
      System.err.println("Given path (" + args(i) + ") does not exist. \n")
      printUsage()
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println("Given paht (" + args(i) + ") is not a file.\n")
      printUsage()
      System.exit(1)
    }

    i += 1
    dfsDirPath = args(i)
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
    parseArgs(args)

    println("Performing local word count.")
    val fileContents = readFile(localFilePath.toString)
    val localWordCount = runLocalWordCount(fileContents)

    println("Creating SparkSession.")
    val spark = SparkSession.builder()
      .appName("DFS Read Write Test")
      .master("local[*]")
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    println("Writing local file to DFS.")
    val dfsFileName = dfsDirPath + "/dfs_read_wirte_test"
    val fileRDD = spark.sparkContext
      .parallelize(fileContents)
    fileRDD.saveAsTextFile(dfsFileName)

    println("Reading file from DFS and running wrod count.")
    val readFileRDD = spark.sparkContext.textFile(dfsFileName)

    val dfsWordCount = readFileRDD.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) agree.")
    } else {
      println(s"Failure! Local Word Count ($localWordCount) " +
        s"and DFS Word Count ($dfsWordCount) disagree.")
    }

    spark.stop()

  }

}




















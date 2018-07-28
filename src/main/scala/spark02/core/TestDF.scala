package spark02.core

import org.apache.spark.sql.SparkSession

object TestDF {
  def createDF(): Unit ={
    val spark = SparkSession
      .builder
      .master("local")
      .appName("df")
      .config("spark.sql.warehouse.dir","D:/mycode1/program/spark/sparksql/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\person.json")
    df.show()

  }

  def main(args: Array[String]): Unit = {
    createDF()
  }
}

package spark02.sql

/**
  * 利用反射机制,获取从RDD获取结构化数据dataframe
  */

import org.apache.spark.sql.SparkSession

case class People(name:String,age:Int){}

object TestDF2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("testDF2")
      .master("local")
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val df = spark.sparkContext.textFile(
      "D:\\mycode1\\program\\spark\\sparksql\\file\\people.txt").map(_.split(","))
      .map(x=>People(x(0),x(1).toInt)).toDF()

    df.show()

    df.select(df("name")).show()

    df.sort(df("age")).show()

    val count = df.select(df("name")).count()
    println(count)

    df.filter(df("age")>25).show()

    df.select(df("name").as("stuName")).show()
  }

}

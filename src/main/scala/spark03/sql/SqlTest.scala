package spark03.sql

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * create by nulijiushimeili on 2018-07-28
  */

case class Log(id:Int, content:String)
object SqlTest {
  def main(args: Array[String]): Unit = {
    val warehouse = new File("file:D:/mycode1/BigData/hello_bigdata/spark-warehouse")
    if (warehouse == null){
      warehouse.mkdir();
    }

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(s"${this.getClass.getName}")
      .config("spark.sql.warehouse.dir",warehouse.getAbsolutePath)
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val rdd = spark.createDataFrame((0 to 99).map(i=> Log(i,s"content_$i")))
    rdd.createOrReplaceTempView("log")

    val res = sql("select * from log")
    res.show()
    res.printSchema()

    res.rdd.map{ row =>
      val id = row.getAs[Int]("id")
      val content = row.getAs[String]("content")
      (id,content)
    }.foreach(println)

    spark.stop()
  }
}

package spark03.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

/**
  * create by nulijiushimeili on 2018-08-01
  */
//case class Log(id:Int,content:String)
object SqlTest2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(s"${this.getClass.getName}")

    val spark = SparkSession.builder()
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val rdd = spark.createDataFrame((0 to 9).map(i => Log(i, s"content_$i"))).toDF("rid", "name")

    rdd.createOrReplaceTempView("log")

    val top = 5

    val sql =
      s"""
         |select * from log
         |where rid <= $top
         |and 1=1
      """.stripMargin // s 函数多行的语法

    val res = spark.sql(sql)

    res.show() // 结果预览
    res.printSchema() // 打印表结构信息

    res.rdd.map {
      case Row(mid: Int, mName: String) => (s"$mid", s"$mName")
    }.foreach(println)


    spark.stop()

  }
}

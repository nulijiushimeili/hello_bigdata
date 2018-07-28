package spark02.sql

import org.apache.spark.sql.{Row, SparkSession}

case class Log(id: Int, content: String)

object SqlTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark sql test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:\\D:\\mycode1\\program\\spark\\spark03\\spark-warehouse")
      .getOrCreate()

    test03(spark)

    spark.stop()

  }

  def test01(spark: SparkSession): Unit = {
    import spark.implicits._

    val rdd = spark.sparkContext.parallelize(0 to 10)
      .map(x => Log(x, s"content_$x"))

    rdd.toDF().createOrReplaceTempView("log")

    import spark.sqlContext

    sqlContext.sql("select * from log").show()


  }

  def test02(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.parallelize(0 to 10)
      .map(x => Log(x, s"content_$x")).toDF()
      .createOrReplaceTempView("log")

    val res = spark.sql("select * from log")

    res.show()

    res.rdd.map { row =>
      //      val id = row.getInt(0)
      //      val content = row.getString(1)

//      val id = row.getAs[Int](0)
//      val content = row.getAs[String](1)

      // 通常使用这种方法最多,字段的个数有时候会改变,但是字段的名字只要符合,
      // 修改字段的个数不会对代码造成影响, 提高代码的鲁棒性
      val id = row.getAs[Int]("id")
      val content = row.getAs[String]("name")
      (id, content)
    }.foreach(println)

    // 打印出模式信息
    println(res.schema)

  }


  def test03(spark: SparkSession): Unit = {
    import spark.implicits._

    spark.sparkContext.parallelize(0 to 10)
      .map(x => Log(x, s"content_$x")).toDF()
      .createOrReplaceTempView("log")

    val res = spark.sql("select * from log")

    res.rdd.map {
      case Row(id:Int,content:String)=>s"$id---$content"
    }.foreach(println)

    // 打印出模式信息
    println(res.schema)

  }
}

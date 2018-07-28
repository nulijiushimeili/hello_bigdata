package spark01.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 在没有创建视图之前每次查询都会向数据库中去读取
  * 对于频繁查询的数据要创建视图,相当于cache
  */
object ReadMysql2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ReadMysql2")
      .config("spark.sql.warehouse.dir","file:\\D:\\\\mycode1\\\\program\\\\spark\\\\sparksql\\\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")

    val res: DataFrame = spark.read.jdbc(
      "jdbc:mysql://localhost:3306/myschool","class", prop)

    res.show()
    res.cache()   // 并没有任何作用
//    * 在没有创建视图之前每次查询都会向数据库中去读取
//      * 对于频繁查询的数据要创建视图,相当于cache
    res.show()

    val sql = "select stuname from class"
    res.createTempView("class")
    val res2: DataFrame = spark.sql(sql)
    res2.show()
    val sql2 = "select stuage from class"
    val res3 = spark.sql(sql2)
    res3.show()

    spark.stop()
  }
}

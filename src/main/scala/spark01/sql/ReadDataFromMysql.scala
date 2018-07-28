package spark01.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 从mysql中读取数据
  */
object ReadDataFromMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReadDataFromMysql")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    //    // Read data
    //    val jdbcDF = spark.read.format("jdbc")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .option("url", "jdbc:mysql://localhost:3306/myschool")
    //      .option("dbtable", "class")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .load()
    //
    //    jdbcDF.show()

    val prop = new Properties()


    prop.put("driver", "com.mysql.jdbc.driver")
    prop.put("user", "root")
    prop.put("password", "123456")

    spark.read.jdbc("jdbc:mysql://localhost:3306/myschool", "class", prop).show()

    spark.stop()
  }

}

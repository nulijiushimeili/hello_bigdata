package spark03.core

import org.apache.spark.sql.SparkSession

/**
  * create by nulijiushimeili on 2018-07-28
  */
object ReadHiveMetastoreDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getName}")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:\\D:\\mycode1\\BigData\\hello_spark\\spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._
    import spark.sql

    sql("show databases").show()


  }
}

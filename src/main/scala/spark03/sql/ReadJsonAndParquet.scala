package spark03.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Operation json file and parquet file use spark sql.
  *
  * create by nulijiushimeili on 2018-07-29
  */
object ReadJsonAndParquet {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName(s"${this.getClass.getName}")

    val spark = SparkSession.builder()
      .config(conf)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._

    val jsonFilePath = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json"
    spark.read.json(jsonFilePath).createOrReplaceTempView("people")
    val rs = spark.sql("select * from people")
    rs.printSchema()
    rs.show()

    val parquetFilePath = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\users.parquet"
    spark.read.parquet(parquetFilePath).createOrReplaceTempView("users")
    val rs2 = spark.sql("select * from users")
    rs2.printSchema()
    rs2.show()

    spark.stop()

  }
}

package spark01.sql

import org.apache.spark.sql.SparkSession

object SqlDataSourceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark sql data source example")
      .config("spark.sql.warehouse.dir","file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    runBasicDataSourceExample(spark)

    spark.stop()
  }


  private def runBasicDataSourceExample(spark:SparkSession): Unit ={
//    val usersDF = spark.read
//      .parquet("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\users.parquet")
//    usersDF.select("name","favorite_color").write
//      .save("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\namesAndFavColors.parquet")

    val peopleDF = spark.read.format("json")
      .load("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\people.json")
    peopleDF.select("name","age").write.format("parquet")
      .save("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\namesAndAges.parquet")

    val peopleDFCsv = spark.read.format("csv")
      .option("sep",";")
      .option("inferSchema","true")
      .option("header","true")
      .load("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\people.csv")

    val sqlDF = spark.sql("select * from parquet.`file:\\D:\\mycode1\\program\\spark\\spark01\\data\\users.parquet`")

    peopleDF.write.partitionBy("favorite_color")
      .format("parquet")
      .save("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\namesPartByColor.parquet")

//    usersDF.write.partitionBy("favorite_color")
//      .bucketBy(42,"name")
//      .saveAsTable("users_parquet_bucketed")

  }
}

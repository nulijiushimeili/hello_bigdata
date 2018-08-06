package spark01.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-06
  */
object ReadCsv {
  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(SparkProperties.master)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._

    // 2. 读取CSV文件将其注册为临时视图
    val schema = StructType(Array(
      StructField("id", IntegerType),
      StructField("lat", StringType),
      StructField("lon", StringType),
      StructField("time", StringType)
    ))
    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\taxi.csv"
    spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .schema(schema)
      .load(path)
      .createOrReplaceTempView("temp_taxi")

    // 3. 获取给定数据的属性数据,并将其注册为临时表
    spark.sql(
      """
        |select
        | id,substring(time,0,2) as hour
        |from temp_taxi
      """.stripMargin)
      .createOrReplaceTempView("temp_id_hour")

    // 4. 计算各个小时,各个出租车的载客次数 ===> 一条数据就一次
    spark.sql(
      """
        |SELECT
        |  id, hour, COUNT(1) as count
        |FROM temp_id_hour
        |GROUP BY id, hour
      """.stripMargin)
      .createOrReplaceTempView("temp_id_hour_count")

    // 5. 计算各个小时中,载客次数最多的前五个出租车的载客情况
    spark.sql(
      """
        |SELECT
        |  id, hour, count,
        |  ROW_NUMBER() OVER (PARTITION BY hour ORDER BY count DESC) as rnk
        |FROM temp_id_hour_count
      """.stripMargin)
      .createOrReplaceTempView("temp_id_hour_count_rnk")
    val resultDF = spark.sql(
      """
        |select
        | id,hour,count
        |from temp_id_hour_count_rnk
        |where rnk <= 5
      """.stripMargin)

    resultDF.cache()

    resultDF.show()
    resultDF.repartition(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(s"file:\\tmp\\${System.currentTimeMillis()}")
    //      .save("hdfs:bigdata-senior02.ibeifeng.com:8020/result/sql/csv/02")

    spark.stop()
  }
}

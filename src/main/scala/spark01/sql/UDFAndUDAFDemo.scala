package spark01.sql

import org.apache.spark.sql.SparkSession
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-07
  */
object UDFAndUDAFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(SparkProperties.master)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Define udf function.
    spark.udf.register("format_double",(value:Double) => {
      import java.math.BigDecimal
      val bd = new BigDecimal(value)
      bd.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue()
    })

    // Register udaf
    spark.udf.register("self_avg",AvgUDAF)

    // 创建模拟数据并注册成为临时表
    spark.sparkContext.parallelize(Array(
      (1, 1234),
      (1, 45212),
      (1, 22125),
      (1, 12521),
      (1, 12352),
      (2, 52352),
      (2, 2232),
      (2, 12521),
      (2, 12323),
      (3, 2253),
      (3, 2233),
      (3, 22558),
      (4, 252),
      (4, 235),
      (5, 523)
    )).toDF("id", "sal").registerTempTable("temp_emp")

    spark.sql(
      """
        |select
        | id,
        | AVG(sal) as sal1,
        | self_avg(sal) as sal2,
        | format_double(self_avg(sal)) as sal3
        | from temp_emp
        | group by id
      """.stripMargin)
      .show()

    spark.stop()
  }
}

package spark01.sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 从mysql数据库中将数据导出到别的数据库中
  */
object MoveData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Demo").
      master("local[*]")
      .config("spark.sql.warehouse.dir","file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/myschool")
      .option("dbtable", "class")
      .option("user", "root")
      .option("password", "123456")
      .load().toDF()

    jdbcDF.show()

    jdbcDF.createTempView("class")

    val sql = "select stuname,stuage from class"

    val df: DataFrame = spark.sql(sql)

    df.show()

//    这个方法有问题
//    // org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider does not allow create table as select.
//    df.write.mode("overwrite").format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/myschool")
//      .option("dbtable", "newta")
//      .option("numPartitions", "4")
//      .option("user", "root")
//      .option("password", "123456")
//      .save()

     // 向数据库中写入数据
    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.driver")
    prop.put("user","root")
    prop.put("password","123456")

    df.write.mode("overwrite").jdbc(
      "jdbc:mysql://localhost:3306/myschool","newtab",prop)

    println("Complete!")

    spark.stop()

  }
}
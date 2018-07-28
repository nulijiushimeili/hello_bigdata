package spark02.sql

import org.apache.spark.sql.SparkSession

object ReadMysqlDB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("connectMySql")
      .master("local")
      .config("spark.sql.warehouse.dir", "D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/myschool")
      .option("dbtable", "class")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .load()

    jdbcDF.show()
    //      +---+-------+------+----------+
    //      | id|stuname|stuage|subject_no|
    //      +---+-------+------+----------+
    //      |  1|    tom|     9|         1|
    //      |  2|    cat|    10|         1|
    //      |  3|   alex|    16|         3|
    //      |  4|   yase|    18|         1|
    //      |  5| Anqila|    15|         2|
    //      |  9|   yase|    18|         1|
    //      | 10| Anqila|    15|         2|
    //      +---+-------+------+----------+


  }
}

package spark03.core

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object SparkSQLDemo{
  def main(args: Array[String]): Unit = {

    // $example on:init_session$
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getName}")
      .master("local")
      .config("spark.sql.warehouse.dir", "file:\\D:\\mycode1\\BigData\\hello_spark\\spark-warehouse")
      .getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._
    import spark.sql

    //下面我们设置两条数据表示两个学生信息
    val studentRDD = spark.sparkContext.parallelize(Array("3 xiaoming male 26", "4 xiaohua male 27")).map(_.split(" "))

    //下面要设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, nullable = true), StructField("name", StringType, nullable = true), StructField("gender", StringType, nullable = true), StructField("age", IntegerType, nullable = true)))

    //下面创建Row对象，每个Row对象都是rowRDD中的一行
    val rowRDD = studentRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).trim, p(3).toInt))

    //建立起Row对象和模式之间的对应关系，也就是把数据和模式对应起来
    val studentDF = spark.createDataFrame(rowRDD, schema)

    //查看studentDF
    studentDF.show()
//      +---+---------+------+---+
//      | id | name | gender | age |
//      +---+---------+------+---+
//      | 3 | Rongcheng | M | 26 |
//      | 4 | Guanhua | M | 27 |
//      +---+---------+------+---+
    //下面注册临时表
    studentDF.createOrReplaceTempView("tempTable")

    sql("select * from tempTable").show()
  }
}
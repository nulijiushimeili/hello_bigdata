package spark02.sql

/**
  * 从mysql中读取数据,
  * 向mysql写入数据
  */

import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ReadMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("readMysql")
      .master("local")
      .config("spark.sql.warehouse.dir", "D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    // Read data
//    val jdbcDF = spark.read.format("jdbc")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("url", "jdbc:mysql://localhost:3306/myschool")
//      .option("dbtable", "class")
//      .option("user", "root")
//      .option("password", "123456")
//      //      .option("","")
//      .load()

    //jdbcDF.show()

    //向MySQL中写入数据
    import spark.implicits._
    //    val stuDF = spark.sparkContext.parallelize(List(
    //      Student(11,"Libai",30,2),
    //      Student(12,"Sunshangxiang",16,1)
    //    )).toDF()
    val studentRDD = spark.sparkContext
      .parallelize(Array("11,Libai,30,2", "12,Sunshangxiang,16,1")).map(_.split(","))
//    studentRDD.foreach(println)

    //定义模式字符串
    val schemaString = "id stuname stuage subject_no"

    //使用模式字符串生成模式
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)
    //
    val rowRDD = studentRDD.map(x=> Row(x(0),x(1),x(2),x(3)))
    //rowRDD.foreach(println)

    //
    val studentDF = spark.createDataFrame(rowRDD,schema)

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")

    //要选择模式为追加
    studentDF.write.mode("append").jdbc(
      "jdbc:mysql://localhost:3306/myschool","class",prop)


  }
}


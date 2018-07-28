package spark02.sql

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object LoadOtherDataSource {
  def main(df:Array[String]): Unit ={
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\src\\file")
      .appName("load other data source")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    //使用spark读取数据库表的信息
    val jdbcDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/myschool")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","class")
      .option("user","root")
      .option("password","123456")
      .load()

    jdbcDF.show
//    +---+-------+------+----------+
//    | id|stuname|stuage|subject_no|
//    +---+-------+------+----------+
//    |  1|    tom|     9|         1|
//    |  2|    cat|    10|         1|
//    |  3|   alex|    16|         3|
//    +---+-------+------+----------+


    //将rdd的数据写入到数据库
    val studentRDD = spark.sparkContext
      .parallelize(Array("9 yase 18 1","10 Anqila 15 2")).map(_.split(" "))

    //设置模式信息
    val schema = StructType(List(
      StructField("id",IntegerType,nullable = true),
      StructField("stuname",StringType,nullable = true),
      StructField("stuage",IntegerType,nullable = true),
      StructField("subject_no",IntegerType,nullable = true)
    ))

    // create Row object,each Row is a line in rowRDD
    val rowRDD = studentRDD.map(p => Row(p(0).toInt,p(1).trim,p(2).toInt,p(3).toInt))

    //create dataframe(建立行和模式之间的关系,也就是把数据和模式对应起来)
    val studentDF = spark.createDataFrame(rowRDD,schema)

    //create property to save connations about jdbc
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("driver","com.mysql.jdbc.Driver")

    //use append , and connect database ,append data to database
    studentDF.write.mode("append")
      .jdbc("jdbc:mysql://localhost:3306/myschool","myschool.class",prop)



  }







}

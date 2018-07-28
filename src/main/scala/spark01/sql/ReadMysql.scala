package spark01.sql

import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ReadMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReadMysql")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.driver")
    prop.put("user","root")
    prop.put("password","123456")

    spark.read.jdbc("jdbc:mysql://localhost:3306/myschool","class",prop).show()

    import spark.implicits._

//    val stu1 = new Student(15,"Luzhishen",35,1)
//    val stu2 = new Student(16,"Haha",34,2)
//    val stu3 = new Student(17,"Liuyong",22,1)
//    val list = List(stu1,stu2,stu3)
//
//    val stuRDD: RDD[Student] = spark.sparkContext.parallelize(list.)
//    stuRDD.foreach(println)
//
//    val schemaString = "id stuname stuage subject_no"
//
//    val fields = schemaString.split(" ").map(fn => StructField(fn,StringType,nullable = true))

//    val schema = StringType(fields)

    val studentRDD = spark.sparkContext
      .parallelize(Array("15,Libai,30,2", "16,Sunshangxiang,16,1")).map(_.split(","))
//    studentRDD.foreach(println)

    //定义模式字符串
    val schemaString = "id stuname stuage subject_no"

    //使用模式字符串生成模式
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)
    //
    val rowRDD = studentRDD.map(x=> Row(x(0),x(1),x(2),x(3)))
    rowRDD.foreach(println)

    val stuDF = spark.createDataFrame(rowRDD,schema)

    //要选择模式为追加
    stuDF.write.mode("append").jdbc(
      "jdbc:mysql://localhost:3306/myschool","class",prop)

    spark.stop()
  }

}

package spark02.sql

/**
  * 使用ORM的方式将数据导入到数据库
  */

import java.util.Properties

import org.apache.spark.sql.SparkSession

case class Student(id: Int, stuName: String, stuage: Int, subject_no: Int)

object DFWriteToMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("writeToMysql")
      .master("local")
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    //加载集合中的数据
    val studentDF = spark.sparkContext.parallelize(List(
      Student(13,"Zhenji",20,1),
      Student(14,"Xiaoqiao",18,2)
    )).toDF()

    //创建配置参数
    val prop = new Properties()
    prop.put("driver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")

    //将数据写入到数据库
    studentDF.write.mode("append")
      .jdbc("jdbc:mysql://localhost:3306/myschool","class",prop)

    Thread.sleep(3000)

    //查看数据库中的数据
    val jdbcDF = spark.read.format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/myschool")
      .option("dbtable","class")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .load()

    jdbcDF.show()
  }
}

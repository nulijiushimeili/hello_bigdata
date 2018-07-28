package spark01.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * 向数据库中写入对象的注意点
  * 1. 使用的对象映射的字段名必须是和数据库的字段名是一致的,否则会报错
  * 2. 注意primary key 不能重复,发生冲突的时候会无法写入
  * 3. 如果中途出现数据导入异常,并没有启用mysql的事务,为发生异常的数据还是会写入到数据库
  */
object DFtoMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ReadMysql2")
      .config("spark.sql.warehouse.dir","file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val stu4 = Student(17,"www",23,1)
    val stu3 = Student(18,"www",23,1)
    val stu1 = Student(20,"www",23,1)
    val stu2 = Student(21,"xxx",23,1)

    val stuDF = spark.sparkContext.parallelize(
//      List(Student(1,"www",23,1),Student(1,"xxx",23,1))
      List(stu3,stu4,stu1,stu2)
    ).toDF()

    stuDF.show()

    val prop = new Properties()
    prop.put("dirver","com.mysql.jdbc.Driver")
    prop.put("user","root")
    prop.put("password","123456")

    stuDF.write.mode("append")
      .jdbc("jdbc:mysql://localhost:3306/myschool","class",prop)

    println("DataFrame write to msyql success!")

    spark.stop()


  }


}

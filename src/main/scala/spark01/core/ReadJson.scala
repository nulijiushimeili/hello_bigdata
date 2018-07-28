package spark01.core

import java.util.Properties

import org.apache.spark.sql.SparkSession

object ReadJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("read json")
      .config("spark.sql.warehouse.dir",
        "file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // read json
    val df = spark.read.json("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\people.json")

    df.show()


    // read mysql
    val prop = new Properties()
    prop.put("dirver","com.mysql.jdbc.Driver")
      prop.put("user","root")
    prop.put("password","123456")

    val df2 = spark.read.jdbc("jdbc:mysql://localhost:3306/myschool","class",prop).toDF()

    df2.show()



    spark.stop()
  }
}

package spark02.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SaveDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("saveDataFrame")
      .master("local")
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val file = spark.sparkContext.textFile(
      "D:\\mycode1\\program\\spark\\sparksql\\file\\people.txt")

    //定义一个模式字符串
    val schemaString3 = "name,age"
    //根据模式字符串生成字段
    val fields = schemaString3.split(",")
      .map(fileName=>StructField(fileName,StringType,nullable = true))
    //根据字段生成模式
    val schema = StructType(fields)
    //根据文件生成行
    val rowRDD = file.map(_.split(",")).map(x=>Row(x(0),x(1)))
    //根据行和模式生成数据结构
    val peopleDF = spark.createDataFrame(rowRDD,schema)
    //根据数据结构生成视图
    peopleDF.createOrReplaceTempView("people")
    //查询结果
    val res = spark.sql("select name,age from people")

    res.show()

    //将结果保存到文件,保存后的文件是一个目录
//    peopleDF.select("name","age")
//      .write.format("csv")
//      .save("D:\\mycode1\\program\\spark\\sparksql\\file\\result\\people")

    //将文件保存为parquet文件
    peopleDF.write.mode(SaveMode.Overwrite)
      .parquet("D:\\mycode1\\program\\spark\\sparksql\\file\\result\\parquet")

    spark.stop()
  }
}

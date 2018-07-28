package spark02.sql

import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{Row, SparkSession, types}

object TestSchemaString2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("testSchemaString")
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

//    1.创建sparkSession
//    2.导入隐式转换
    import spark.implicits._
//    3.加载文件
    val file = spark.sparkContext.textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\people.txt")
//    4.定义模式字符串
    val schemaString2 = "name age"
//    5.根据模式字符串生成字段
    val field = schemaString2.split(" ")
      .map(fieldName=> StructField(fieldName,StringType,nullable = true))
//    6.根据字段生成模式
    val schema = types.StructType(field)
//    7.将加载的数据生成数据行
    val rowRDD = file.map(_.split(",")).map(x=> Row(x(0),x(1)))
//    8.根据行和模式生成dataframe
    val peopleDF = spark.createDataFrame(rowRDD,schema)
//    9.生成视图
    peopleDF.createOrReplaceTempView("people")
//    10.执行查询
    val res = spark.sql("select name from people")

    res.show()

  }
}

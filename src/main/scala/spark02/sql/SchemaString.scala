package spark02.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaString {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SchemaString")
      .master("local")
      .config("spark.sql.warehouse.dir", "D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val file = spark.sparkContext.textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\person.txt")

    val schemaString = "name,age"

    //根据模式字符串生成字段
    val fields = schemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    //生成数据行
    val rowRDD = file.map(_.split(",")).map(x => Row(x(0).split("\"")(1), x(1)))

    //根据字段生成模式
    val schema = StructType(fields)

    //根据行和模式生成dataframe
    val personDF = spark.createDataFrame(rowRDD, schema)

    //生成视图
    personDF.createOrReplaceTempView("person")

    //使用SQL查询数据
    val res = spark.sql("select name from person")

    res.show()


    spark.stop()

  }
}

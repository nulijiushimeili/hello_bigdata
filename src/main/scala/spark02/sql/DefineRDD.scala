package spark02.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DefineRDD {
  def main(dff:Array[String]): Unit ={

    //create SparkSession
    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .appName("define a RDD by code")
      .master("local")
      .getOrCreate()

    //start transform dataframe
    import spark.implicits._

    //
    val peopleRDD = spark.sparkContext
      .textFile("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\people.txt")

    //定义个一个模式字符串
    val schemaString = "name age"

    //根据模式字符串生成字段
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName,StringType, nullable = true))

    //根据字段生成模式
    val schema = StructType(fields)

    //生成行
    val rowRDD = peopleRDD.map(_.split(",")).map(x => Row(x(0),x(1)))

    //根据数据行和模式生成dataframe
    val peopleDF = spark.createDataFrame(rowRDD,schema)

    //生成视图
    peopleDF.createOrReplaceTempView("people")

    //执行SQL查询
    val result = spark.sql("select name from people")

    result.show()



  }
}

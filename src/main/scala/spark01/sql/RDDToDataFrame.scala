package spark01.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-06
  */
object RDDToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(SparkProperties.master)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._

    /**
      * 官方文档：http://spark.apache.org/docs/1.6.1/sql-programming-guide.html#interoperating-with-rdds
      *
      * 转换方法一：利用反射机制 ==> 要求RDD中的数据类型必须是case class的数据类型 + 引入sqlContext中的隐式转换函数
      */
    val rdd = spark.sparkContext.parallelize(Array(
      Person(1, "Tom", 15),
      Person(2, "Mark", 18),
      Person(3, "Jack", 21),
      Person(4, "Katty", 16)
    ))

    // toDF 这个函数常用于列重命名
    val df = rdd.toDF() // 以属性名作为DataFrame的列名称
    val df2 = rdd.toDF("p_id", "p_name", "p_age") // 明确给定列名称,要求顺序一致
    df.show()
    df2.show()

    spark.sparkContext.parallelize(Array(
      Person(1, "Tom", 15),
      Person(2, "Mark", 18),
      Person(3, "Jack", 21),
      Person(4, "Katty", 16)
    )).toDF("id","name","age").show()

    /**
      * 转换方法二: 利用明确给定schema和rdd进行RDD的创建
      */
    val rowRDD: RDD[Row] = spark.sparkContext.parallelize(Array(
      Person(1, "Tom", 15),
      Person(2, "Mark", 18),
      Person(3, "Jack", 21),
      Person(4, "Katty", 16)
    )).map {
      case Person(id, name, age) => {
        Row(id, name, age)
      }
    }
    val schema = StructType(Array(
      StructField("id3",IntegerType),
      StructField("name3",StringType),
      StructField("age3", IntegerType)
    ))
    val df3 = spark.createDataFrame(rowRDD,schema)
    df3.show()

  }
}

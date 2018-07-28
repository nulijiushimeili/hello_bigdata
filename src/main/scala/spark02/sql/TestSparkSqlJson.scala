package spark02.sql

import org.apache.spark.sql.SparkSession


case class people(name:String,age:Int)

object TestSparkSqlJson {
  def main(l:Array[String]): Unit ={

    //create SparkSession
    val spark = SparkSession.builder
      .master("local")
      .appName("read json")
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    //隐式转换
    import spark.implicits._

    //create dataframe
    val df = spark.read.json("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\testsql.json")
    df.show()

    //print schema in a tree format
    df.printSchema()

    //select only the "name" colume
    df.select("name").show

    //count people by age\
    df.groupBy("age").count.show

    //register the dataframe as a temporary view
    df.createOrReplaceTempView("people")

    // query by sql
    val query = spark.sql("select * from people")
    query.show

  }
}

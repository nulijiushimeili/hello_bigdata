package spark02.sql

import org.apache.spark.sql.SparkSession


object TestSparkSql {
  def main(sdf:Array[String]): Unit ={
    val spark = SparkSession
      .builder
      .appName("testSparkSql")
      .master("local")
      .config("spark.sql.warehouse.dir","D:/mycode1/program/spark/sparksql/spark-warehouse")
      .getOrCreate()


    //导入隐式转换,使rdd可以被转换成dataframe
    import spark.implicits._

    val df = spark.read.json("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\testsql.json")
    df.show
    df.printSchema()

    //对某一列进行操作
    df.select(df("name"),df("age")+1).show

    //对某一列的值进行过滤
    df.filter(df("age")>12).show

    //groupBy()进行分组
    df.groupBy(df("age")).count.show

    //排序
    df.sort(df("age").asc).show



  }
}

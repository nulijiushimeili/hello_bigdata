package spark02.sql

import org.apache.spark.sql.SparkSession

object TestDF1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TestDF1")
      .master("local[4]")
      .config("spark.sql.warehouse.dir", "D:/mycode1/program/spark/sparksql/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read.json("D:\\mycode1\\program\\spark\\sparksql\\file\\person.json")

    //查看dataframe
    df.show()

    //查看模式信息
    println(df.schema)

    //选择要查看的列
    df.select(df("name")).show()

    //对查询结果进行排序
    df.select(df("age")).sort().show()

    //给列名取别名
    df.select(df("name").as("username")).show()

    //多字段查询
    df.select(df("name"), df("age")).show()

    //查看第二行的内容
    df.show(1)


  }
}

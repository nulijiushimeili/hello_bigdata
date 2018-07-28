package spark01.sql

import org.apache.spark.sql.SparkSession

case class Person(id:Int, name:String , age:Int)

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("sql")
      .config("spark.sql.warehouse.dir","file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val lineRDD = spark.sparkContext.textFile("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\person.txt")
      .flatMap(_.split("\n"))

        val personRDD = lineRDD.map(x => Person(x.split(",")(0).toInt, x.split(",")(1), x.split(",")(2).toInt))

        val personDF = personRDD.toDF()

    personDF.show()

    personDF.createOrReplaceTempView("person")


    spark.sqlContext.sql("select * form person").show()


  }
}

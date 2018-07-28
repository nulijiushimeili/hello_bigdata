package spark02.sql

/**
  * //利用反射机制推断rdd模式
  */

import org.apache.spark.sql.SparkSession

case class Person(name:String,age:Int)

object Df_Reflect {
  def main(df:Array[String]): Unit ={
    val spark = SparkSession.builder
      .appName("sparksql:create dataframe by case class")
      .master("local")
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    //sparkcontext只能有一个,所以在这里是sparksession.sparkcontext
    //注意上面导入sparksession.implicits._  否则不能完成隐式转换rdd,即toDF()这个方法
    val personDF = spark.sparkContext
      .textFile("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\person.txt")
      .map(_.split(","))
      .map(x => Person(x(0),x(1).trim.toInt)).toDF()

    //dataframe 必须注册临时表才可以使用
    val df = personDF.createOrReplaceTempView("person")

    val personRDD = spark.sql("select * from person where age > 20")
    personRDD.show
      //输出结果如下
//    +-------+---+
//    |   name|age|
//    +-------+---+
//    |  "amy"| 35|
//    | "mara"| 42|
//    |"linas"| 23|
//    |"janny"| 32|
//    +-------+---+

    val count = spark.sql("select count(name) from person").collect().head.getLong(0)
    println(count)




    spark.stop()

  }
}

package spark02.sql

import org.apache.spark.sql.SparkSession


object TestToDF {
  def main(lj: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local")
      .appName("toDF")
      .config("spark.sql.warehouse.dir", "D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("D:\\mycode1\\program\\spark\\sparksql\\file\\people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    //register the dataframe as a temproray view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teernagerDF = spark.sql("select name,age from people where age between 13 and 19")
    teernagerDF.show

    // The columns of a row in the result can be accessed by field index
    teernagerDF.map(x => "name : " + x(0)).show()

    //


    spark.stop()


  }
}

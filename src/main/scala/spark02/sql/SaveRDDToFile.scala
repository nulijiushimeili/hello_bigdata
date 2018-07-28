package spark02.sql

import org.apache.spark.sql.SparkSession

object SaveRDDToFile {
  def main(df:Array[String]): Unit ={
    //将rdd保存成文件
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .master("local")
      .appName("save rdd to file ")
      .getOrCreate()

    import spark.implicits._
    val peopleDF = spark.read.format("json")
      .load("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\people.json")

    //将数据以CSV格式保存,保存后是一个目录
//    peopleDF.select("name","age").write.format("csv").save("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\newpeople.csv")

    //将rdd保存成txt文本文件
    val df = peopleDF.toDF()
    df.rdd.saveAsTextFile("D:\\mycode1\\program\\spark\\sparksql\\src\\file\\newpeople.txt")


  }
}

package spark02.sql

import org.apache.spark.sql.SparkSession



object TestToDS {
  def main(l:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local")
      .appName("toDS")
      .config("spark.sql.warehouse.dir","D:\\mycode1\\program\\spark\\sparksql\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    //将集合转换为dataset
    val caseClassDS = Seq(Person("xiaoming",15)).toDS()
    caseClassDS.show()

    //dataset
    val primitiveDS = Seq(1,2,3).toDS()
    val arr = primitiveDS.map(_ + 1).collect() //返回的是一个数组
    arr.foreach(println)

    //DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    //数据集可以通过提供一个类来转换成数据集。映射将通过名称来完成
    val path = "D:\\mycode1\\program\\spark\\sparksql\\src\\file\\people.json"
//    val peopleDS = spark.read.json(path).as[Person]
//    peopleDS.show()





    spark.stop()
  }
}

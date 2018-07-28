package spark01.core

import org.apache.spark.sql.{SaveMode, SparkSession}

case class Record(key: Int, value: String)

object RDDRelation {
  def main(args: Array[String]) {
    // $example on:init_session$
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("RDDRelation")
      .config("spark.sql.warehouse.dir", "file:\\D:\\mycode1\\program\\spark\\spark01\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))

    df.createOrReplaceTempView("records")

    // 查询所有的结果集
    println("Result of select * : ")
    spark.sql("select * from records").collect().foreach(println)

      // 查看数据集的总条数
    val count = spark.sql("select count(*) from records").collect().head.getLong(0)
    println("COUNT(*): " + count)

    // 格式化输出结果
    val rddFromSql = spark.sql("select key,value from records where key < 10")
    println("Result of RDD map: " )
    rddFromSql.rdd.map(row => s"key:${row(0)}, value:${row(1)}").collect().foreach(println)


    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)


    df.write.mode("overwrite").parquet("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\pair.parquet")

//    val parquetFile = spark.read.parquet("file:\\D:\\mycode1\\program\\spark\\spark01\\data\\pair.parquet")
//
//    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)
//
//    parquetFile.createOrReplaceTempView("parquetFile")
//    spark.sql("select * from parquetFile").collect().foreach(println)

    spark.stop()
  }
}

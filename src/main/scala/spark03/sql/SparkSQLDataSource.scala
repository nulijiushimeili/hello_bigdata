package spark03.sql

import org.apache.spark.sql.SparkSession

/**
  * create by nulijiushimeili on 2018-08-04
  */
object SparkSQLDataSource {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    runBasicDataSourceExample(spark)
    runBasicParquetExample(spark)
    runJsonDatasetExample(spark)
    runParquetSchemaMergingExample(spark)

    spark.stop()
  }


  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    val usersDF = spark.read.load("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\users.parquet")
    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

    val peopleDF = spark.read.format("json")
      .load("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json")
    peopleDF.select("name", "age").write.format("parquet").save("namesAndAge.parquet")

    val sqlDF =
      spark.sql("select * from parquet.`file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\users.parquet`")

  }

  private def runBasicParquetExample(spark: SparkSession): Unit = {
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val peopleDF = spark.read.json("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.parquet("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.parquet")


    // Read in the parquet file created above.
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.parquet")

    // Parquet files can also bu used to create a temporary view and the used in sql statements.
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("select name from parquetFile where age between 13 and 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
  }

  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory.
    val squareDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i * i)).toDF("value", "square")
    squareDF.write.parquet("data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column.
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    cubesDF.write.parquet("data/test_table/key=2")

    // Read the partitioned table
    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()
    // The final schema consists of all 3 columns in the Parquet files
    // together with partitioning column appeared in the partition directory paths .

  }

  private def runJsonDatasetExample(spark: SparkSession): Unit = {
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files.
    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json"
    val peopleDF = spark.read.json(path)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()

    // Creates a temporary view using DataFrame.
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark.
    val teenagerNameDF = spark.sql("select name from people where age between 13 and 19")
    teenagerNameDF.show()

    // Alternatively, a DataFrame can be created for a JSON DataSet
    // represented by an RDD[String] storing one JSON object per string
    val otherPeopleRDD = spark.sparkContext.makeRDD(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil
    )
    val otherPeople = spark.read.json(otherPeopleRDD)
    otherPeople.show()
  }

}

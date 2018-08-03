package spark03.sql

import org.apache.spark.sql.{Encoder, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * create by nulijiushimeili on 2018-08-03
  */
object SparkSql {

  /** Note: Case class in Scala 2.10 can support only up to 22 fields.
    * To work around this limit, you can use custom classes that implement the Product interface. */
  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    runBasicDataFrameExample(spark)
    runDatasetCreation(spark)
    runInferSchema(spark)
    runProgrammaticSchema(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val df = spark.read.json("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json")

    // Displays the content of the DataFrame to stdout.
    df.show()

    // This import is needed to use the $-notation
    import spark.implicits._

    // Print the schema in a tree format.
    df.printSchema()

    // Select only "name" column.
    df.select("name").show()

    // Select everybody, but increment age by 1
    df.select($"name", $"age" + 1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // Count people by age.
    df.groupBy("age").count().show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

  }

  private def runDatasetCreation(spark: SparkSession): Unit = {
    import spark.implicits._

    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2,3,4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }

  private def runInferSchema(spark: SparkSession): Unit = {
    // For implicit conversions from a text file, convert it to a Dataframe
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark.
    val teenagersDF = spark.sql("select name,age from people where age between 13 and 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name : " + teenager(0)).show()

    teenagersDF.map(teenager => "Name : " + teenager.getAs[String]("name")).show()

    // No pre-defined encoders for Dataset[Map[K,V]],define explicitly
    implicit val mapEncoder: Encoder[Map[String, Any]] = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    // Primintive types and case classes can be also defined as
    implicit val stringIntMapEncoder: Encoder[Map[String, Int]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String,T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin","age" -> 19))
  }


  private def runProgrammaticSchema(spark: SparkSession): Unit = {
    import spark.implicits._
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("file:\\D:\\mycode1\\BigData\\hello_bigdata\\data\\people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD,schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("select name from people")

    // The result of SQL queries are DataFrame and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    results.map(attribute => "Name : " + attribute(0)).show()

  }

}

package spark01.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-07
  */

case class Person2(name: String, age: Int, sex: String, salary: Int, deptNo: Int)

case class Dept(dept: Int, deptName: String)

object SparkSQLDSLDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(SparkProperties.master)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    spark.udf.register("sexToNum", (sex: String) => {
      sex.toUpperCase match {
        case "M" => 0
        case "F" => 1
        case _ => -1
      }
    })
    // Use spark udf function.
    spark.udf.register("self_avg", AvgUDAF)

    val rdd1 = spark.sparkContext.parallelize(Array(
      Person2("张三", 21, "M", 1235, 1),
      Person2("李四", 20, "F", 1235, 1),
      Person2("王五", 26, "M", 1235, 1),
      Person2("小明", 25, "F", 1225, 1),
      Person2("小花", 24, "F", 1425, 1),
      Person2("小华", 23, "M", 1215, 1),
      Person2("gerry", 22, "F", 1415, 2),
      Person2("tom", 21, "F", 1855, 2),
      Person2("lili", 20, "F", 1455, 2),
      Person2("莉莉", 18, "M", 1635, 2)
    ))
    val rdd2 = spark.sparkContext.parallelize(Array(
      Dept(1, "部门1"),
      Dept(2, "部门2")
    ))

    val person2DF = rdd1.toDF()
    val deptDF = rdd2.toDF()
    /*============================DSL===============================*/
    // 对于多次使用的DataFrame,使用缓存提高效率
    person2DF.cache()
    deptDF.cache()

    // select syntax
    println("-----------------select---------------------------")
    person2DF.select("name", "age", "sex").show()
    person2DF.select($"name", $"age", $"sex".as("sex1")).show
    person2DF.select(col("name").as("emp_name"), col("age"), col("sex")).show()
    person2DF.selectExpr("name", "age", "sex", "sexToNum(sex) as sex_num").show()

    // where / filter
    println("-----------where/filter--------------")
    person2DF.where("age > 22").where("sex = 'M'").where("deptNo = 1").show()
    person2DF.where("age > 20 and sex = 'M' and deptNo = 1").show()
    person2DF.where($"age" > 20 && $"sex" === "M" && $"deptNo" === 1).show()
    person2DF.where($"age" > 20 && $"sex" === "M" && ($"sex" =!= "F")).show()

    // sort
    println("--------------------sort---------------------------")
    // global sort
    person2DF.sort("salary").select("name", "salary").show()
    person2DF.sort($"salary".desc).select("name", "age", "age").show()
    person2DF.sort($"salary".desc, $"salary".asc).select("name", "age", "age").show()
    person2DF.orderBy($"salary".desc, $"salary".asc).select("name", "age", "age").show()
    person2DF.repartition(5)
      .orderBy($"salary".desc, $"age")
      .select("name", "salary", "age").show

    // group by
    println("-----------------------group by---------------------------")
    person2DF.groupBy("sex")
      .agg(
        "salary" -> "avg",
        "salary" -> "sum"
      ).show()
    person2DF.groupBy("sex")
      .agg(
        avg("salary").as("avg_salary"),
        min("salary").as("min_salary"),
        count(lit(1)).as("cnt")
      ).show()
    //    person2DF.groupBy("sex")
    //      .agg(
    //        // 使用自定义的udf函数
    //        "salary" -> "self_avg"
    //      ).show()

    // limit
    person2DF.limit(2).show

    // join
    println("-----------------------join-----------------------------")
    person2DF.join(deptDF).show
    person2DF.join(deptDF.toDF("col1", "deptName"), $"deptNo" === $"col1", "inner").show()
    person2DF.join(deptDF,"deptNo").show()
    person2DF.join(deptDF.toDF("deptNo","name"),Seq("deptNo"),"left_outer")
      .toDF("no","name","age","sex","sal","dname")
      .show()

    // ==窗口分析函数==
    /** *
      * 按照deptNo分组，组内按照salary进行排序，获取每个部门前3个销售额的用户信息
      * select *
      * from
      * (select *, ROW_NUMBER() OVER (Partition by deptNo Order by salary desc) as rnk
      * from person) as tmp
      * where tmp.rnk <= 3
      */
    val w = Window.partitionBy("deptNo").orderBy($"salary".desc,$"age".asc)
    person2DF.select(
      $"name",$"age",$"deptNo",$"salary",
      row_number().over(w).as("rnk")
    ).where("rnk <= 3").show()

    person2DF.unpersist()
    deptDF.unpersist()

  }
}

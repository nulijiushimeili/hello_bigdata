package spark03.sql

import java.io.File

/**
  * create by nulijiushimeili on 2018-07-29
  */
object SparkProperties {
  def warehouseDir(): String ={
    val warehouse = new File("file:D:/mycode1/BigData/hello_bigdata/spark-warehouse")
    if (warehouse == null){
      warehouse.mkdir()
    }
    warehouse.getName
  }

  val warehouse = "spark.sql.warehouse.dir"

  val master = "local[*]"
}

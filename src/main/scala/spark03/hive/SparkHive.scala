package spark03.hive

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import spark03.sql.SparkProperties

/**
  * create by nulijiushimeili on 2018-08-03
  *
  * 如果需要读取并操作hive表的数据,需要编译的时候指定支持hive,否则无法应用此功能
  */
object SparkHive {

  case class Record(key:Int,value:String)

  def main(args: Array[String]): Unit = {
    /**
      * When working with Hive, one must instantiate `SparkSession` with Hive support, including
      * connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
      *      functions. Users who do not have an existing Hive deployment can still enable Hive support.
      * When not configured by the hive-site.xml, the context automatically creates `metastore_db`
      * in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
      * which defaults to the directory `spark-warehouse` in the current directory that the spark
      * application is started.
      */

    val spark = SparkSession.builder()
      .appName("spark hive")
      .master("local[*]")
      .config(SparkProperties.warehouse,SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("create table if not exists src(key INT, value String")
    sql("load data local inpath '/opt/datas/kv1.txt' into table src")

    sql("select * from src").show()

    sql("select count(*) from src").show()

    val sqlDF = sql("select key ,value from src where key < 10 order by key")

    val stringDS = sqlDF.map{
      case Row(key : Int , value:String) => s"key : $key, value : $value"
    }

    stringDS.show()

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i,s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    sql("select * from records r join src s on r.key = s.key").show()

    spark.stop()

  }
}

package spark01.sql

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}
import spark03.sql.SparkProperties

/**
  * 这里的spark应用需要使用到HIVE的元数据，根据hive.metastore.uris参数采用不同的策略：
  * -1. hive.metastore.uris没有给定具体值，那么需要将hive的元数据相关的jdbc的配置信息添加到spark的configration中，
  * 并将元数据库的驱动添加到当前项目的classpath中
  * -2. hive.metastore.uris给定了具体值的情况下，那么需要将hive.metastore.uris的配置信息添加到spark的configration中，
  * 并且启动对应机器上的hive的metastore服务
  * 将hive-site.xml添加到当前spark应用的classpath中，然后根据不同的值采用不同的策略
  *
  * create by nulijiushimeili on 2018-08-06
  */
object HiveJoinMysqlOnSparkSQL {
  def main(args: Array[String]): Unit = {
    // 定义常量信息
    val url = "jdbc:mysql://localhost:3306/test"
    val password = "root"
    val user = "123456"
    val props = new Properties()
    props.put("user", user)
    props.put("password", password)

    // 1. Create SparkSession
    val spark = SparkSession.builder()
      .appName(this.getClass.getName)
      .master(SparkProperties.master)
      .config(SparkProperties.warehouse, SparkProperties.warehouseDir())
      .getOrCreate()

    import spark.implicits._

    /**
      * 需求一: 将hive中的数据同步到mysql中
      * 通过write.jdbc将数据写出到RDBMS有一个小问题,一般情况下不使用该方式将数据洗出到RDBMS中
      * 无法实现insert or update的操作(基于数据记录更新和插入的操作)
      * 最好使用RDD的方式写入到RDBMS
      */
    spark
      .read
      .table("common.dept")
      .write
      .mode(SaveMode.Overwrite)
      .jdbc(url, "tb_dept", props)


    /**
      * 需求二: hive表和mysql中进行join操作
      * 步骤:
      *  1. 读取mysql中的数据形成DataFrame
      *  2. 将DataFrame注册成为临时表(temp_tab_dept)
      * 注意点: 注册临时表的时候不能出现"."这种符号
      *  3. 将temp_tab_dept和common.emp进行join
      */
    spark
      .read
      .jdbc(url, "tb_dept", Array("deptno < 25", "deptno >= 25 and deptno < 28", "deptno >= 28"), props)
      .createOrReplaceTempView("temp_tab_dept")

    val joinResultDF = spark.sql(
      """
        |select a.*,b.dname,b.loc from common.emp a join tmp_tb_dept b on a.deptno = b.deptno
      """.stripMargin)

    /**
      * 需求三: 将数据保存到hdfs,保存格式为parquet
      */
    joinResultDF
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("deptno")
      // 指定的是hdfs上的文件，如果没有给定fs.defaultFS，默认是保存到本地的
      .save("hdfs://bigdata-senior02.ibeifeng.com:8020/result/sql/parquet/01")

    /**
      * 将数据存储到hive中,保存格式为parquet
      */
    joinResultDF
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .partitionBy("deptno")
      .saveAsTable("tb_result_join_emp_dept")

    spark.stop()
  }
}

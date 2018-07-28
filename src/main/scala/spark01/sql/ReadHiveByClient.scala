package spark01.sql

import java.sql.DriverManager

object ReadHiveByClient {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    val conn = DriverManager.getConnection(
      "jdbc:hive2://bigdata-senior02.ibeifeng.com:10000/hadoop14","user","123456")

    val stat = conn.createStatement()

    val sql = "select * from emp"

    val res = stat.executeQuery(sql)

    while(res.next()){
      println(res.getString(2))
    }

  }
}

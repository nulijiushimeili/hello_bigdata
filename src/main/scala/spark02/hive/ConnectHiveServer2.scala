package spark02.hive

/**
  * 连接hiveserver2,
  */

import java.sql.DriverManager

object ConnectHiveServer2 {
  def main(args: Array[String]): Unit = {
    val driver = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)
    val conn = DriverManager.getConnection(
      "jdbc:hive2://bigdata-senior01.ibeifeng.com:10000/db_emp","hello","123")
    val st = conn.createStatement()
    val sql = "show tables"
    val res = st.executeQuery(sql)
    while(res.next())println(res.getString(1))

  }
}

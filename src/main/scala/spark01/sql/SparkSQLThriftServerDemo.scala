package spark01.sql

import java.sql.DriverManager

/**
  * Connect spark sql thrift server.
  *
  * create by nulijiushimeili on 2018-08-07
  */
object SparkSQLThriftServerDemo {
  def main(args: Array[String]): Unit = {
    // 1. Create driver
    val driver = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driver)

    // 2. Create connection
    val (url,username,password) = ("jdbc:hive2://bigdata-senior02.ibeifeng.com","hello","123456")
    val conn = DriverManager.getConnection(url,username,password)

    // 3. execute sql
    val sql = "select * from common.emp a join common.dept b on a.deptno = b.deptno"
    val stmt = conn.prepareStatement(sql)
    val rs = stmt.executeQuery()

    while(rs.next()){
      println(rs.getString("ename") + ":" + rs.getDouble("sal"))
    }
    rs.close()
    stmt.close()

    println("======================================================================================")

    val sql2 = "select avg(sal) as avg_sal,deptno from common.emp group by deptno having avg_sal > 8000"

    val stmt2 = conn.prepareStatement(sql2)
    val rs2 = stmt2.executeQuery()
    while(rs2.next()){
      println(rs2.getInt("deptno") + ":" + rs2.getDouble("avg_sal"))
    }
    rs2.close()
    stmt.close()

    conn.close()
  }
}

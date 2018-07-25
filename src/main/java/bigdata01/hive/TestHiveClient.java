package bigdata01.hive;

import java.sql.*;

/**
 * 使用jdbc连接hive仓库和连接mysql数据的操作一模一样
 * 操作的时候,必须开启hiveserver2,否则会连不上
 */
public class TestHiveClient {

    private static final String driverName = "org.apache.bigdata01.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Connection conn = DriverManager.getConnection
                ("jdbc:hive2://bigdata-senior02.ibeifeng.com:10000/hadoop14");
        Statement st = conn.createStatement();

        String sql = "show tables";
        System.out.println("Running:" + sql);
        ResultSet rs = st.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

        if (rs != null) rs.close();
        if (st != null) st.close();
        if (conn != null) conn.close();

    }
}

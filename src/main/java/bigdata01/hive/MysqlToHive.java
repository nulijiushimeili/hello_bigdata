package bigdata01.hive;

import java.sql.*;
import java.util.ArrayList;

/**
 * transform data from mysql to bigdata01.hive
 * 这种蛋疼的方法不要用
 */
public class MysqlToHive {
    private String mysqlURL = "jdbc:mysql://localhost:3306/myschool";
    private String mysqlUser = "root";
    private final String mysqlPassword = "123456";
    private String hiveURL = "jdbc:hive2://bigdata-senior02.ibeifeng.com:10000/hadoop14";
    private Connection conn;
    private Statement stat;
    private ResultSet rs;


    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        MysqlToHive mysqlToHive = new MysqlToHive();
        mysqlToHive.readMysql();
        mysqlToHive.readHive();
        mysqlToHive.toHive(mysqlToHive.readMysql());
    }

    private ArrayList<Student> readMysql() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(mysqlURL, mysqlUser, mysqlPassword);
        String sql = "select * from class";
        stat = conn.createStatement();
        rs = stat.executeQuery(sql);
        ArrayList<Student> arrayList = new ArrayList<>();
        while (rs.next()) {
//            arrayList.add(
//                    new Student(Integer.valueOf(rs.getString(1)),
//                    rs.getString(2),
//                    Integer.valueOf(rs.getString(3)),
//                    Integer.valueOf(rs.getString(4))));
            arrayList.add(
                    new Student(rs.getInt(1),
                            rs.getString(2),
                            rs.getInt(3),
                            rs.getInt(4)));
        }

        for (Student stu : arrayList) {
            System.out.println(stu);
        }
        return arrayList;
    }

    private void readHive() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.bigdata01.hive.jdbc.HiveDriver");
        conn = DriverManager.getConnection(hiveURL, "user", "123456");
        stat = conn.createStatement();
        String sql = "select * from emp";
        rs = stat.executeQuery(sql);
        ArrayList<Emp> arrayList = new ArrayList<>();
        while (rs.next()) {
            arrayList.add(
                    new Emp(
                            rs.getInt(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4),
                            rs.getString(5),
                            rs.getInt(6),
                            rs.getInt(7),
                            rs.getInt(8)
                    ));
        }

        for (Emp emp : arrayList) {
            System.out.println(emp);
        }

    }

    private void toHive(ArrayList<Student> arrayList) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.bigdata01.hive.jdbc.HiveDriver");
        conn = DriverManager.getConnection(hiveURL, "user", "123456");
        String createTable = "create table Student (id int, name string, age int, subject_no int)" +
                "row format delimited fields terminated by '\t' stored as textfile";
        stat = conn.createStatement();
//        成功返回false? 失败抛异常?
        stat.execute(createTable);

    }
}


class Student {
    private int id;
    private String stuname;
    private int age;
    private int subject_no;

    public Student(int id, String stuname, int age, int subject_no) {
        this.id = id;
        this.stuname = stuname;
        this.age = age;
        this.subject_no = subject_no;
    }

    @Override
    public String toString() {
        return "Student{" +
                "id=" + id +
                ", stuname='" + stuname + '\'' +
                ", age=" + age +
                ", subject_no=" + subject_no +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getStuname() {
        return stuname;
    }

    public void setStuname(String stuname) {
        this.stuname = stuname;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSubject_no() {
        return subject_no;
    }

    public void setSubject_no(int subject_no) {
        this.subject_no = subject_no;
    }
}

class Emp {
    private int id;
    private String name;
    private String work;
    private String leader;
    private String birth;
    private int sal;
    private int com;
    private int dept_no;

    public Emp(int id, String name, String work, String leader, String birth, int sal, int com, int dept_no) {
        this.id = id;
        this.name = name;
        this.work = work;
        this.leader = leader;
        this.birth = birth;
        this.sal = sal;
        this.com = com;
        this.dept_no = dept_no;
    }

    @Override
    public String toString() {
        return "Emp{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", work='" + work + '\'' +
                ", leader='" + leader + '\'' +
                ", birth='" + birth + '\'' +
                ", sal=" + sal +
                ", com=" + com +
                ", dept_no=" + dept_no +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getWork() {
        return work;
    }

    public void setWork(String work) {
        this.work = work;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public String getBirth() {
        return birth;
    }

    public void setBirth(String birth) {
        this.birth = birth;
    }

    public int getSal() {
        return sal;
    }

    public void setSal(int sal) {
        this.sal = sal;
    }

    public int getCom() {
        return com;
    }

    public void setCom(int com) {
        this.com = com;
    }

    public int getDept_no() {
        return dept_no;
    }

    public void setDept_no(int dept_no) {
        this.dept_no = dept_no;
    }
}
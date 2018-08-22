package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestCreateNewFile {
    public static void main(String[] args) {
        try {
            test01();
            test02();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用绝对路径
     *
     * @throws IOException
     */
    private static void test01() throws IOException{
        FileSystem fs = HdfsUtil.getFileSystem();
        boolean created = fs.createNewFile(new Path("/hdfs/api/5.txt"));
        System.out.println(created ? "created success!" : "created failed");
        fs.close();
    }

    private static void test02() throws IOException{
        FileSystem fs = HdfsUtil.getFileSystem();
        boolean created = fs.createNewFile(new Path("5.txt"));
        System.out.println(created ? "created success!" : "created failed");
        fs.close();
    }
}

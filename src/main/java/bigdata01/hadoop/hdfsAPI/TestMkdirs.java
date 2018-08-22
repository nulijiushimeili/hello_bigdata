package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestMkdirs {
    public static void main(String[] args) {
        try {
            test01();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void test01() throws IOException{
        FileSystem fs = HdfsUtil.getFileSystem();
        boolean mkdirsed = fs.mkdirs(new Path("/hdfs/api/mkdirs"));
        System.out.println(mkdirsed);
        fs.close();
    }
}

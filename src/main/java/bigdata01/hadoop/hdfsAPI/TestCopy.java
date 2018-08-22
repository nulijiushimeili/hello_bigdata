package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestCopy {
    public static void main(String[] args) {
        try {
            testCopyFromLocal();
            testCopyToLocal();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void testCopyFromLocal() throws Exception{
        FileSystem fs = HdfsUtil.getFileSystem();
        fs.copyFromLocalFile(new Path("D://tmp/test.txt"),new Path("/hdfs/api/2.txt"));
        fs.close();
    }

    static void testCopyToLocal() throws Exception{
        FileSystem fs = HdfsUtil.getFileSystem();
        fs.copyToLocalFile(new Path("/hdfs/api/2.txt"),new Path("D://tmp/3.txt"));
        fs.close();
    }
}

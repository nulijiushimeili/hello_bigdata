package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestGetFileStatus {
    public static void main(String[] args) {
        try {
            FileSystem fs = HdfsUtil.getFileSystem();
            FileStatus status = fs.getFileStatus(new Path("/hdfs/api/1.txt"));
            System.out.println(status.isDirectory() ? "is a directory" : "is a file");
            System.out.println("committed time: " + status.getAccessTime());
            System.out.println("复制因子: " + status.getReplication());
            System.out.println("Length: " + status.getLen());
            System.out.println("Last modify: " + status.getModificationTime());
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

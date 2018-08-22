package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestDelete {
    public static void main(String[] args) {
        try{
            testDelete();
            testDeleteOnExit();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private static void testDelete() throws IOException{
        FileSystem fs  = HdfsUtil.getFileSystem();
        boolean deleted = fs.delete(new Path("/hdfs/api/1.txt"),true);
        System.out.println(deleted ? "delete file success" : "delete file failed");
        deleted = fs.delete(new Path("/hdfs/api/4.txt"),false);
        System.out.println(deleted ? "delete file success" : "delete file failed");
        deleted = fs.delete(new Path("/hdfs/api/mkdirs"),false);
        System.out.println(deleted ? "delete directory success" : "delete directory failed");
        fs.close();
    }

    /**
     * 在系统推出之后才删除文件
     */
    private static void testDeleteOnExit() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        boolean deleted = fs.delete(new Path("/hdfs/api/2.txt"),true);
        System.out.println("Delete file by 'delete' method : " + deleted);
        deleted = fs.deleteOnExit(new Path("/hdfs/api/5.txt"));
        System.out.println("DeleteOnExit : " + deleted);
        System.in.read();
        fs.close();
    }
}

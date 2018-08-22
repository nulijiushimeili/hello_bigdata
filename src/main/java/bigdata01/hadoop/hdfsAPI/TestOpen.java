package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestOpen {
    public static void main(String[] args) {
        try {
            test01();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void test01() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        InputStream is = fs.open(new Path("/hdfs/api/1.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while((line = br.readLine())!= null ){
            System.out.println(line);
        }
        br.close();
        is.close();
        fs.close();
    }
}

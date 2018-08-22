package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestAppend {
    public static void main (String[] args){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bigdata-senior02.ibeifeng.com:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path("/hdfs/api/1.txt");
            FSDataOutputStream dos = fs.append(path);
            dos.write("hello word!".getBytes());
            dos.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

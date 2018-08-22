package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class TestCreate {
    public static void main(String[] args) {
        try {
            test1();
            test2();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void test1() throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bigdata-senior02.ibeifeng.com:8020");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream dos = fs.create(new Path("/beifeng/api/1.txt"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
        bw.write("hello hadoop api.");
        bw.newLine();
        bw.write("hello hadoop!");
        bw.close();
        dos.close();
        fs.close();
    }

    static void test2() throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bigdata-senior02.ibeifeng.com:8020");
        FileSystem fs = FileSystem.get(conf);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/hfds/api/4.txt"),(short)1)));
        bw.write("Running away with your wife.");
        bw.close();
        fs.close();
    }
}

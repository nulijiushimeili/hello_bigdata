package bigdata01.hadoop.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * create by nulijiushimeili on 2018-08-21
 */
public class HdfsUtil {
    public static Configuration getConfiguration(){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bigdata-senior02.ibeifeng.com:8020");
        return conf;
    }

    public static FileSystem getFileSystem() throws IOException{
        return getFileSystem(getConfiguration());
    }

    public static FileSystem getFileSystem(Configuration conf ) throws IOException{
        return FileSystem.get(conf);
    }
}

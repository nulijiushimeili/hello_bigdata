package bigdata01.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 自定义UDF函数
 */

public class BigDataUDF extends UDF {
    //默认是转化成小写
    public Text evaluate(Text str) {
        return this.evaluate(str, new IntWritable(0));
    }

    //进行转换
    public Text evaluate(Text str, IntWritable flag) {
        if (str != null) {
            if (flag.get() == 0) {
                return new Text(str.toString().toLowerCase());
            } else if (flag.get() == 1) {
                return new Text(str.toString().toUpperCase());
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public static void main(String[] args) {
        //test toLowerCase()
//        System.out.println(new BigDataUDF().evaluate(new Text("HADOOP")));
        //test toUpperCase()
        System.out.println((new BigDataUDF().evaluate(new Text("hadoop"), new IntWritable(1))));
    }
}

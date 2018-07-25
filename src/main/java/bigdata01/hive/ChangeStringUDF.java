package bigdata01.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * 自定义UDF函数,完成大小写转换
 */
public class ChangeStringUDF extends UDF {

    public Text evaluate(Text str){
        return evaluate(str,new IntWritable(0));
    }

    public Text evaluate(Text str, IntWritable flag){
        if(str != null){
            if(flag.get() == 0){
                return new Text (str.toString().toUpperCase());
            }else{
                return new Text(str.toString().toLowerCase());
            }
        }
        return null;
    }

    public static void main(String[] args) {
        // test toUpperCase()
        System.out.println(new ChangeStringUDF().evaluate(new Text("hello")));
        // test toLowwerCase()
        System.out.println(new ChangeStringUDF().evaluate(new Text("HELLO WORD!"), new IntWritable(3)));
    }


}

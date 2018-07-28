package bigdata01.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * create by nulijiushimeili on 2018-07-28
 *
 * 获取专题name
 * 用法: getTopic(URL,pattern)
 */
public class GetCommonParam extends UDF {

    public String evaluate(String url, String pattern){
        if(url == null || url.trim().length() == 0){
            return null;
        }

        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(url);

        if(m.find()){
            return m.group(0).toLowerCase();
        }
        return null;
    }

    public int evaluate(int type){
        return 0;
    }

    public static void main(String [] args){
        GetCommonParam gp = new GetCommonParam();
        System.out.println(gp.evaluate("http://item.yhd.com/item/50235346?tc","item/[0-9]+"));
    }
}

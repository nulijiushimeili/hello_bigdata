package bigdata01.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * create by nulijiushimeili on 2018-07-28
 *
 * url = "http://cms.yhd.com/sale/IhSwTYNxnzS?tc=ad.0.0.15116-32638141.1&tp=1.1.708.0.3.LEHaQW1-10-35dOM&ti=4FAK"
 *
 * getSaleName.pattern = "sale/[a-zA-Z0-9]+"
 */
public class GetSaleName extends UDF {

    public String evaluate(String url,String pattern){
        if (url == null || url.trim().length() == 0){
            return null;
        }

        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(url);

        if (m.find()){
            return m.group(0).toLowerCase().split("/")[1];
        }else{
            return null;
        }
    }

    public static void main(String[] args) {
        String url = "http://cms.yhd.com/sale/IhSwTYNxnzS?tc=ad.0.0.15116-32638141.1&tp=1.1.708.0.3.LEHaQW1-10-35dOM&ti=4FAK";
        String pattern = "sale/[a-zA-Z0-9]+";
        GetSaleName getSaleName = new GetSaleName();
        System.out.println(getSaleName.evaluate(url,pattern));
    }
}

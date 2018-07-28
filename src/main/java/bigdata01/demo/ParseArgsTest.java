package bigdata01.demo;

import java.io.File;
import java.util.Map;

/**
 * create by nulijiushimeili on 2018-07-28
 *
 * This is a class for parse args from main() method.
 */

class ParseArgs {
    private Map<String,String> map = null;

    public ParseArgs(String[] args){
        if (args.length == 0){
            return;
        }

        int i = 0;
        while(i < args.length){
            String par = args[i].trim();
            if(par.startsWith("-")){
                String key = par.substring(1).trim();
                i++;
                String value = null;
                if(args.length>i){
                    value = args[i].trim();
                    if(value.startsWith("\"") || value.startsWith("\'")){
                        value = value.substring(1,value.length()-1).trim();
                    }
                }
                map.put(key,value);
            }
        }

    }

    public Map<String ,String> getMap(){
        return this.map;
    }
}


public class ParseArgsTest{
    public static void main(String[] args) {
//        args = new String[3];
//        args[0]="/opt/datas/aa.sql";
//        args[1]="-date";
//        args[2]="20180708";

        try {
            ParseArgs pa = new ParseArgs(args);
            String sql = ParseSQLUtils.getSql(new File(args[0]));
            String str = ParseSQLUtils.parse(sql,pa.getMap());
            System.out.println(str);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

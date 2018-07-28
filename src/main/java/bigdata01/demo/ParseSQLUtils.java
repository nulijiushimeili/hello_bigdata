package bigdata01.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

/**
 * create by nulijiushimeili on 2018-07-28
 */
public class ParseSQLUtils {
    private static final String BEGIN="${";
    private static final String END="}";

    // 把SQL文件逐行读出,拼成字符串返回
    public static String getSql(File file )throws Exception{
        BufferedReader br = new BufferedReader(new FileReader(file));
        StringBuffer sqlBuffer = new StringBuffer();
        String temp = null;
        while((temp=br.readLine())!= null){
            String tmp = temp.trim();
            if(tmp.length()==0 || tmp.startsWith("#") || tmp.startsWith("--")){
                continue;
            }
            sqlBuffer.append(tmp).append(" ");
        }
        br.close();
        return sqlBuffer.toString();
    }


    /**
     *
     * @param sql       SQL statement
     * @param map       parameters in command.
     * @return          put parameters into sql .
     */
    public static String parse(String sql,Map<String,String> map){
        int begin = sql.indexOf(BEGIN);
        while(begin != -1){
            String suffix = sql.substring(begin + BEGIN.length());
            int end = begin + BEGIN.length() + suffix.indexOf(END);
            String key = sql.substring(begin+BEGIN.length(),end).trim();
            if(map != null && map.get(key) != null){
                sql = sql.substring(0,begin) + map.get(key) + sql.substring(end+1,sql.length());
            }else{
                throw new RuntimeException("Invalid Exception ......");
            }
            begin = sql.indexOf(BEGIN);
        }
        return sql;
    }
}

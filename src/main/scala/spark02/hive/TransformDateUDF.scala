package spark02.hive

import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.io.Text

object TransformDateUDF {
  /**
    * UDF函数转换日期的格式
    * 当前日期格式: 31/Aug/2015:00:04:37 +0800
    * 期望的日期格式: 2015-08-31 00:04:37
    */
  def evaluate(time:Text):String= {
    // set i/o Date format
    val inputDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH)
    val outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // If time is null or empty String, return null
    if(time == null) null
    if(StringUtils.isBlank(time.toString)) null

    //去除字段中的双引号
    val parser = time.toString.replaceAll("\"","")

    //解析日期,格式化成自己期望的格式
    val parseDate = inputDate.parse(parser)
    outputDate.format(parseDate)
  }

  def main(args: Array[String]): Unit = {
    println(evaluate(new Text("31/Aug/2015:00:04:37 +0800")))
  }
}

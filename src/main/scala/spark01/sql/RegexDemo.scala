package spark01.sql

import java.util.regex.Pattern

import scala.util.matching.Regex

/**
  * create by nulijiushimeili on 2018-08-07
  */
object RegexDemo {
  def main(args: Array[String]): Unit = {
    matchStandardDate()
    matchStandardDateWithScala()
  }

  def matchStandardDate(): Unit={
    val regex="""\d{4}-\d{2}-\d{2} \d{2}:\d\d:\d\d"""
    val str = "2018-05-05 12:12:12"
    val p = Pattern.compile(regex)
    val m = p.matcher(str)
    if (m.find()){
      println(str)
    }else{
      println("Nothing to matched.")
    }
  }

  def matchStandardDateWithScala(): Unit ={
    val regex: Regex = """\d{4}-\d{2}-\d{2} \d{2}:\d\d:\d\d""".r
    val str = "2018-05-05 12:12:12"
    if(regex.findFirstMatchIn(str).nonEmpty){
      println(str)
    }else{
      println("Nothing to matched.")
    }
  }
}

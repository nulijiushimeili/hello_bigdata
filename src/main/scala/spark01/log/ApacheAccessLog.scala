package spark01.log

import scala.util.matching.Regex

/**
  * create by nulijiushimeili on 2018-08-06
  *
  * 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846
  */
case class ApacheAccessLog(
                            ipAddress: String, // ip地址
                            client: String, // 客户端唯一标识
                            userId: String, // 用户唯一标识
                            serverTime: String, // 服务器时间
                            method: String, // 请求类型/方式
                            endpoint: String, // 请求的资源
                            protocol: String, // 请求的协议名称
                            responseCode: Int, // 请求返回值
                            contentSize: Long // 返回结果数据大小
                          )

object ApacheAccessLog {
  // Apache日志正则
  val PARTTERN: Regex =
    """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  /**
    * 验证数据是否符合给定的日志正则,符合返回true,否则返回false
    *
    * @param line Log info
    * @return If matched regex return true, otherwise return false.
    */
  def isValidateLogLine(line: String): Boolean = {
    val options = PARTTERN.findFirstMatchIn(line)

    if (options.isEmpty) {
      false
    } else {
      true
    }
  }

  /**
    * 解析输入的日志数据
    *
    * @param line Log info
    * @return Parse log and return ApacheAccessLog class.
    */
  def parseLogLine(line: String): ApacheAccessLog = {
    if (!isValidateLogLine(line)) {
      throw new IllegalArgumentException("Data type is not matched.")
    }

    // Get data from match line.
    val options = PARTTERN.findFirstMatchIn(line)

    // Get matcher
    val matcher: Regex.Match = options.get

    // Create return value.
    ApacheAccessLog(
      matcher.group(1), // 获取匹配字符串中第一个小括号中的值
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8).toInt,
      matcher.group(9).toLong
    )
  }
}

package spark01.streaming


//import org.apache.log4j.{Level, Logger}
//
//import org.apache.spark.internal.Logging
//
///** Utility functions for Spark Streaming examples. */
//object StreamingExamples extends Logging {
//
//  /** Set reasonable logging levels for spark.streaming if the user has not configured log4j. */
//  def setStreamingLogLevels() {
//    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
//    if (!log4jInitialized) {
//      // We first log something to initialize Spark's default logging, then we override the
//      // logging level.
//      logInfo("Setting log level to [WARN] for spark.streaming example." +
//        " To override add a custom log4j.properties1 to the classpath.")
//      Logger.getRootLogger.setLevel(Level.WARN)
//    }
//  }
//}

package spark03.streaming

import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging

/**
  * create by nulijiushimeili on 2018-08-04
  */

/** Utility functions for Spark Streaming examples. */
object StreamingExamples extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}


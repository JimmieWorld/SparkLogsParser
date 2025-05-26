package org.testTask.parser.processors

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.testTask.parser.Session

import scala.io.Source
import java.io.File

object RawDataProcessor {

  def process(
      sc: SparkContext,
      filesPath: String,
      errorStatsAcc: ErrorStatsAccumulator
  ): RDD[Session] = {
    val dir = new File(filesPath)

    val files = dir.listFiles().toSeq
    val sessionLogsRDD = sc.parallelize(files)

    sessionLogsRDD.map { file =>
      val source = Source.fromFile(file, "Windows-1251")
      try {
        val lines = source.getLines().toList.iterator.buffered
        val context = ParsingContext(lines, errorStatsAcc, file.getName)
        Session.extract(context)
      } finally {
        source.close()
      }
    }
  }
}

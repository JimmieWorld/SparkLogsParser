package org.testtask.parser.processors

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.testtask.Utils
import org.testtask.parser.Session
import org.testtask.parser.processors.ErrorStatsAccumulator.ErrorStats

import scala.io.Source
import java.io.File

object RawDataProcessor {
  private val sc: SparkContext = initSparkContext()
  private val errorStats: ErrorStatsAccumulator = new ErrorStatsAccumulator()

  def process(
      filesPath: String = "src/main/resources/data"
  ): RDD[Session] = {
    sc.register(errorStats)

    val dir = new File(filesPath)
    val files = dir.listFiles().toSeq
    val sessionLogsRDD = sc.parallelize(files)

    sessionLogsRDD.map { file =>
      val source = Source.fromFile(file, "Windows-1251")
      try {
        val lines = source.getLines().toList.iterator.buffered
        val context = ParsingContext(lines, errorStats, file.getName)

        Session.extract(context)
      } finally {
        source.close()
      }
    }
  }

  private def initSparkContext(): SparkContext = {
    val conf = new SparkConf()
      .setAppName("SessionProcessor")
      .setMaster("local[*]")
      .set("spark.ui.enabled", "false")

    new SparkContext(conf)
  }

  private def saveErrorStatistics(
      errorStats: ErrorStats,
      outputPath: String
  ): Unit = {

    val errors = errorStats.map { case (errorType, (count, samples)) =>
      (errorType, count.toString, samples.mkString(" -|- "))
    }.toArray

    Utils.saveToCSV(
      errors,
      outputPath,
      Array("ErrorType", "Count", "Samples")
    )(_.productIterator.map(_.toString).toArray)
  }

  def stop(outputPath: String = "src/main/resources/results/errors.csv"): Unit = {
    saveErrorStatistics(errorStats.value, outputPath)
    sc.stop()
  }
}

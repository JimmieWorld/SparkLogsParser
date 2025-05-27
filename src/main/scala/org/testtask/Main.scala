package org.testtask

import org.apache.spark.{SparkConf, SparkContext}
import org.testtask.tasks._
import org.testtask.parser.processors.ErrorStatsAccumulator.ErrorStats
import org.testtask.parser.processors.{ErrorStatsAccumulator, RawDataProcessor}


object Main {

  def main(args: Array[String]): Unit = {
    val sc = initSparkContext()
    val errorStatsAcc = new ErrorStatsAccumulator()
    sc.register(errorStatsAcc)

    try {
      val sessionsRDD =
        RawDataProcessor.process(sc, "src/main/resources/data", errorStatsAcc)

      Task1.execute(sessionsRDD)
      Task2.execute(sessionsRDD)
    } finally {
      saveErrorStatistics(errorStatsAcc.value, "src/main/resources/results/errors.csv")
      sc.stop()
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

}

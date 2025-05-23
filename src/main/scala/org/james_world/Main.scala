package org.james_world

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.james_world.ErrorStatsAccumulatorDef.ErrorStats
import org.james_world.extractor.RawDataProcessor
import org.james_world.tasks._

import java.io.File

object Main {

  def main(args: Array[String]): Unit = {
    val sc = initSparkContext()
    val errorStatsAcc = new ErrorStatsAccumulator()
    sc.register(errorStatsAcc)

    try {
      val sessionsRDD =
        processFiles(sc, filePath = "src/main/resources/data", errorStatsAcc)

      val res1 = Task1.execute(sessionsRDD, "ACC_45616")
      val res2 = Task2.execute(sessionsRDD)

      println(s"Task1: $res1")
      Utils.saveToCSV(
        res2,
        "src/main/resources/results/task2_results.csv"
      )(item => item.productIterator.map(_.toString).toArray)

      saveErrorStatistics(errorStatsAcc.value, "src/main/resources/results/errors.csv")

    } catch {
      case e: Exception =>
        errorStatsAcc.add((s"${e.getClass.getName}", s"[main] ${e.getMessage}"))
    } finally {
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

  private def processFiles(
      sc: SparkContext,
      filePath: String,
      errorStatsAcc: ErrorStatsAccumulator
  ): RDD[Session] = {
    val dir = new File(filePath)

    val files = dir.listFiles().toSeq
    val filesRDD = sc.parallelize(files)

    RawDataProcessor.process(filesRDD, errorStatsAcc)
  }

  private def saveErrorStatistics(
      errorStats: ErrorStats,
      outputPath: String
  ): Unit = {

    val errorArray = errorStats.map { case (errorType, (count, samples)) =>
      (errorType, count.toString, samples.mkString(" -|- "))
    }.toArray

    Utils.saveToCSV(
      data = errorArray,
      outputPath = outputPath,
      header = Some(Array("ErrorType", "Count", "Samples"))
    )(item => item.productIterator.map(_.toString).toArray)
  }

}

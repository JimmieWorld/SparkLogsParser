package org.testtask.parser.processors

import org.apache.spark.util.AccumulatorV2
import org.testtask.parser.processors.ErrorStatsAccumulator.ErrorStats

import scala.collection.mutable

object ErrorStatsAccumulator {
  type ErrorStats = Map[String, (Int, List[String])]
}

class ErrorStatsAccumulator extends AccumulatorV2[(String, String), ErrorStats] {

  private val errorType2Stats = mutable.Map.empty[String, (Int, List[String])]

  override def isZero: Boolean = errorType2Stats.isEmpty

  override def copy(): AccumulatorV2[(String, String), ErrorStats] = {
    val newAcc = new ErrorStatsAccumulator
    newAcc.errorType2Stats ++= errorType2Stats
    newAcc
  }

  override def reset(): Unit = errorType2Stats.clear()

  override def add(v: (String, String)): Unit = {
    val (errorType, line) = v

    val (count, samples) = errorType2Stats.getOrElse(errorType, (0, Nil))
    val updatedSamples = if (samples.length < 10) samples :+ line else samples

    errorType2Stats.put(errorType, (count + 1, updatedSamples))
  }

  override def merge(
      other: AccumulatorV2[(String, String), ErrorStats]
  ): Unit = {
    other match {
      case o: ErrorStatsAccumulator =>
        o.errorType2Stats.foreach { case (errType, (count, samples)) =>
          val (currentCount, currentSamples) =
            errorType2Stats.getOrElse(errType, (0, Nil))
          val mergedSamples = (currentSamples ++ samples).take(10)
          errorType2Stats.put(errType, (currentCount + count, mergedSamples))
        }
      case _ => ()
    }
  }

  override def value: ErrorStats = errorType2Stats.toMap
}

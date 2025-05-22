package org.james_world

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

object ErrorStatsAccumulatorDef {
    type ErrorStats = Map[String, (Int, List[String])]
}

import ErrorStatsAccumulatorDef.ErrorStats

class ErrorStatsAccumulator extends AccumulatorV2[(String, String), ErrorStats] {

    private val errorMap = mutable.Map.empty[String, (Int, List[String])]

    override def isZero: Boolean = errorMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), ErrorStats] = {
        val newAcc = new ErrorStatsAccumulator
        newAcc.errorMap ++= errorMap
        newAcc
    }

    override def reset(): Unit = errorMap.clear()

    override def add(v: (String, String)): Unit = {
        val (errorType, line) = v

        val (count, samples) = errorMap.getOrElse(errorType, (0, Nil))
        val updatedSamples = if (samples.length < 10) samples :+ line else samples

        errorMap.put(errorType, (count + 1, updatedSamples))
    }

    override def merge(other: AccumulatorV2[(String, String), ErrorStats]): Unit = {
        other match {
            case o: ErrorStatsAccumulator =>
                o.errorMap.foreach { case (errType, (count, samples)) =>
                    val (currentCount, currentSamples) = errorMap.getOrElse(errType, (0, Nil))
                    val mergedSamples = (currentSamples ++ samples).take(10)
                    errorMap.put(errType, (currentCount + count, mergedSamples))
                }
            case _ => ()
        }
    }

    override def value: ErrorStats = errorMap.toMap
}
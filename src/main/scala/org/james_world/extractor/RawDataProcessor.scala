package org.james_world.extractor

import scala.io.Source
import org.apache.spark.rdd.RDD
import scala.collection.BufferedIterator
import org.james_world.{ErrorStatsAccumulator, ParsingContext, Session}

import java.io.File

object RawDataProcessor {

  def process(
      filesRDD: RDD[File],
      errorStatsAcc: ErrorStatsAccumulator
  ): RDD[Session] = {
    filesRDD.map { file =>
      val lines: BufferedIterator[String] = {
        val source = Source.fromFile(file, "Windows-1251")
        try {
          val list = source.getLines().toList
          list.iterator.buffered
        } catch {
          case e: Exception =>
            errorStatsAcc.add((s"${e.getClass.getName}", s"[RawDataProcessor] ${e.getMessage}"))
            Iterator.empty.buffered
        } finally {
          source.close()
        }
      }

      val context = ParsingContext(lines, errorStatsAcc)
      Session.extract(context, file.getName)
    }
  }
}

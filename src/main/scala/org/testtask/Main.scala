package org.testtask

import org.testtask.tasks._
import org.testtask.parser.processors.RawDataProcessor

object Main {

  def main(args: Array[String]): Unit = {
    try {
      val sessionsRDD = RawDataProcessor.process()

      Task1.execute(sessionsRDD)
      Task2.execute(sessionsRDD)
    } finally {
      RawDataProcessor.stop()
    }
  }
}

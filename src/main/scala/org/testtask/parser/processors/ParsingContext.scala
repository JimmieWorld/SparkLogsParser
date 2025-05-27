package org.testtask.parser.processors

import scala.collection.BufferedIterator

case class ParsingContext(
    lines: BufferedIterator[String],
    errorStatsAcc: ErrorStatsAccumulator,
    fileName: String
) {
  val sessionBuilder: SessionBuilder = SessionBuilder(fileName)
}

package org.testtask.parser.processors

import scala.collection.BufferedIterator

case class ParsingContext(
    lines: BufferedIterator[String],
    errorStats: ErrorStatsAccumulator,
    fileName: String
) {
  val sessionBuilder: SessionBuilder = SessionBuilder(fileName)
}

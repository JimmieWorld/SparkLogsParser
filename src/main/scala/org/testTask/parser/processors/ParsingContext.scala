package org.testTask.parser.processors

import java.time.LocalDateTime
import scala.collection.BufferedIterator
import scala.collection.mutable

case class ParsingContext(
    lines: BufferedIterator[String],
    errorStatsAcc: ErrorStatsAccumulator,
    fileName: String,
    searchTimestamps: mutable.Map[String, LocalDateTime] = mutable.Map.empty
)

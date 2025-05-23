package org.james_world

import scala.collection.BufferedIterator

case class ParsingContext(
    bufferedIt: BufferedIterator[String],
    errorStatsAcc: ErrorStatsAccumulator
)

package org.james_world.Events

import org.james_world.ErrorStatsAccumulator
import java.time.LocalDateTime
import scala.collection.BufferedIterator

trait Event {
    def timestamp: Option[LocalDateTime]
}

trait EventParser {
    def parse(lines: BufferedIterator[String], errorStatsAcc: ErrorStatsAccumulator): Option[Event]
}
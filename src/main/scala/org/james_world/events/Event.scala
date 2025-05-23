package org.james_world.events

import org.james_world.{ErrorStatsAccumulator, ParsingContext}

import java.time.LocalDateTime
import scala.collection.BufferedIterator

trait Event {
    def timestamp: Option[LocalDateTime]
}

trait EventParser {
    def parse(context: ParsingContext): Event
}
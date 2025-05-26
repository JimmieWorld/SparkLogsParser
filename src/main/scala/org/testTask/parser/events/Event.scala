package org.testTask.parser.events

import org.testTask.parser.processors.ParsingContext

import java.time.LocalDateTime

trait Event {
    def timestamp: Option[LocalDateTime]
}

trait EventParser {
    def parse(context: ParsingContext): Event
}
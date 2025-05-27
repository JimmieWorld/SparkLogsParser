package org.testtask.parser.events

import org.testtask.parser.processors.ParsingContext

import java.time.LocalDateTime

trait Event {
    def timestamp: Option[LocalDateTime]
}

trait EventParser {
    def parse(context: ParsingContext): Unit
}
package org.james_world.Events

import java.time.LocalDateTime

trait Event {
    def timestamp: LocalDateTime
}

trait EventParser {
    def parse(lines: Seq[String]): Option[Event]
}
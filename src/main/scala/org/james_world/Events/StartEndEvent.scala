package org.james_world.Events

import org.james_world.Extractors.DateTimeExtractor
import java.time.LocalDateTime

trait TimestampedEventParser extends EventParser {
    def createEvent(timestamp: LocalDateTime): Event

    override def parse(lines: Seq[String]): Option[Event] = {
        if (lines.isEmpty) None
        else Some(createEvent(DateTimeExtractor.extractTimestamp(lines.head)))
    }
}

case class SessionStartEvent(timestamp: LocalDateTime) extends Event
object SessionStartEvent extends TimestampedEventParser {
    override def createEvent(timestamp: LocalDateTime): Event = SessionStartEvent(timestamp)
}

case class SessionEndEvent(timestamp: LocalDateTime) extends Event
object SessionEndEvent extends TimestampedEventParser {
    override def createEvent(timestamp: LocalDateTime): Event = SessionEndEvent(timestamp)
}
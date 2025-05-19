package org.james_world

import org.james_world.Events.{
    CardSearchEvent, DocumentOpenEvent, Event,
    EventParser, QuickSearchEvent, SessionStartEvent,
    SessionEndEvent
}
import java.time.LocalDateTime

case class Session(
    sessionId: String,
    events: Seq[Event]
)

object Session {
    def processLines(lines: Seq[String], fileName: String): Session = {
        val firstLine = lines.headOption.map(_.trim).getOrElse("")

        if (firstLine.isEmpty) {
            return Session(sessionId = fileName, events = Seq.empty)
        }

        lines.tail.foldLeft((Seq.empty[Event], Seq(lines.head))) { case ((events, buffer), line) =>
            val trimmedLine = line.trim
            val updatedBuffer = buffer :+ trimmedLine

            if (isNextEvent(trimmedLine)) {
                val newEvent = getParserForLine(buffer.head).flatMap(_.parse(buffer)).toSeq
                newEvent.foreach(updateSearchTimestamp)
                (events ++ newEvent, Seq(trimmedLine))
            } else {
                (events, updatedBuffer)
            }
        } match {
            case (events, buffer) =>
                val finalEvents = if (buffer.nonEmpty) {
                    getParserForLine(buffer.head).flatMap(_.parse(buffer)).toSeq
                } else {
                    Seq.empty
                }
                val allEvents: Seq[Event] = events ++ finalEvents
                val uniqueEvents: Seq[Event] = allEvents.distinct

                Session(sessionId = fileName, events = uniqueEvents)
        }
    }

    private def isNextEvent(line: String): Boolean = {
        line.startsWith("DOC_OPEN") ||
        line.startsWith("CARD_SEARCH_START") ||
        line.startsWith("QS") ||
        line.startsWith("SESSION_END") ||
        line.startsWith("SESSION_START")
    }

    private def getParserForLine(line: String): Option[EventParser] = {
        val parsers: Map[String, EventParser] = Map(
            "SESSION_START" -> SessionStartEvent,
            "CARD_SEARCH" -> CardSearchEvent,
            "QS" -> QuickSearchEvent,
            "DOC_OPEN" -> DocumentOpenEvent,
            "SESSION_END" -> SessionEndEvent
        )

        parsers.keys.find(line.startsWith).map(parsers)
    }

    private def updateSearchTimestamp(event: Event): Unit = {
        event match {
            case qs: QuickSearchEvent if qs.timestamp != LocalDateTime.of(0, 1, 1, 0, 0, 0) =>
                val searchTimestamp = Map(qs.searchId -> qs.timestamp)
                DocumentOpenEvent.addSearchTimestamp(searchTimestamp)
            case cs: CardSearchEvent if cs.timestamp != LocalDateTime.of(0, 1, 1, 0, 0, 0) =>
                val searchTimestamp = Map(cs.searchId -> cs.timestamp)
                DocumentOpenEvent.addSearchTimestamp(searchTimestamp)
            case _ => ()
        }
    }
}
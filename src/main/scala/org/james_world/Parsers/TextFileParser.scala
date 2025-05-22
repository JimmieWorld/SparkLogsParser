package org.james_world.Parsers

import org.james_world.Events.{CardSearchEvent, DocumentOpenEvent, Event, EventParser, QuickSearchEvent}
import org.james_world.Extractors.DateTimeExtractor
import org.james_world.{ErrorStatsAccumulator, Session}
import java.time.LocalDateTime

object TextFileParser {
    def processLines(lines: Seq[String], fileName: String, errorStatsAcc: ErrorStatsAccumulator): Session = {
        val bufferedIt = lines.iterator.buffered

        var cardSearches = Seq.empty[CardSearchEvent]
        var quickSearches = Seq.empty[QuickSearchEvent]
        var docOpens = Seq.empty[DocumentOpenEvent]

        var sessionStart: Option[LocalDateTime] = None
        var sessionEnd: Option[LocalDateTime] = None

        while (bufferedIt.hasNext) {
            val line = bufferedIt.head
            val splitLine = line.split("\\s+")

            if (line.startsWith("SESSION_START")) {
                sessionStart = DateTimeExtractor.extractTimestamp(splitLine.last, "SESSION_START", errorStatsAcc)
                bufferedIt.next()
            } else if (line.startsWith("SESSION_END")) {
                sessionEnd = DateTimeExtractor.extractTimestamp(splitLine.last, "SESSION_END", errorStatsAcc)
                bufferedIt.next()
            } else {
                getParserForLine(line) match {
                    case Some(parser) =>
                        parser.parse(bufferedIt, errorStatsAcc) match {
                            case Some(event) =>
                                updateSearchTimestamp(event)

                                event match {
                                    case cs: CardSearchEvent =>
                                        cardSearches = cardSearches :+ cs
                                    case qs: QuickSearchEvent =>
                                        quickSearches = quickSearches :+ qs
                                    case doo: DocumentOpenEvent =>
                                        docOpens = docOpens :+ doo
                                    case _ => ()
                                }

                            case None =>
                                errorStatsAcc.add(("ParseFailed", s"[$fileName] Failed to parse event at line: $line"))
                                bufferedIt.next()
                        }

                    case None =>
                        errorStatsAcc.add(("NoParserFound", s"[$fileName] No parser found for line: $line"))
                        bufferedIt.next()
                }
            }
        }

        Session(
            sessionId = fileName,
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = cardSearches,
            quickSearches = quickSearches,
            docOpens = docOpens
        )
    }

    private def getParserForLine(line: String): Option[EventParser] = {
        val parsers: Map[String, EventParser] = Map(
            "CARD_SEARCH" -> CardSearchEvent,
            "QS" -> QuickSearchEvent,
            "DOC_OPEN" -> DocumentOpenEvent
        )

        parsers.keys.find(line.startsWith).map(parsers)
    }

    private def updateSearchTimestamp(event: Event): Unit = {
        event match {
            case qs: QuickSearchEvent if qs.timestamp.isDefined =>
                val searchTimestamp = Map(qs.searchId -> qs.timestamp)
                DocumentOpenEvent.addSearchTimestamp(searchTimestamp)
            case cs: CardSearchEvent if cs.timestamp.isDefined =>
                val searchTimestamp = Map(cs.searchId -> cs.timestamp)
                DocumentOpenEvent.addSearchTimestamp(searchTimestamp)
            case _ => ()
        }
    }
}

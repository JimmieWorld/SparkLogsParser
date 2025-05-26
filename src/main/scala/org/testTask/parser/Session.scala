package org.testTask.parser

import org.testTask.parser.events.utils.{DateTimeParser, DocOpenLinker}
import org.testTask.parser.events.{CardSearch, DocumentOpen, Event, EventParser, QuickSearch}
import org.testTask.parser.processors.{ParsingContext, SessionBuilder}

import java.time.LocalDateTime

case class Session(
    sessionId: String,
    sessionStart: Option[LocalDateTime],
    sessionEnd: Option[LocalDateTime],
    cardSearches: Seq[CardSearch],
    quickSearches: Seq[QuickSearch],
    docOpens: Seq[DocumentOpen]
)

object Session {
  def extract(context: ParsingContext): Session = {
    val lines = context.lines
    val errorStatsAcc = context.errorStatsAcc
    val fileName = context.fileName

    var sessionStart: Option[LocalDateTime] = None
    var sessionEnd: Option[LocalDateTime] = None

    val builder = SessionBuilder(context)

    try {
      while (lines.hasNext) {
        val line = lines.head
        val splitLine = line.split("\\s+")

        if (line.startsWith("SESSION_START")) {
          sessionStart = DateTimeParser.parseTimestamp(splitLine.last, errorStatsAcc, fileName)
          lines.next()
        } else if (line.startsWith("SESSION_END")) {
          sessionEnd = DateTimeParser.parseTimestamp(splitLine.last, errorStatsAcc, fileName)
          lines.next()
        } else {
          getParserForLine(line).foreach { parser =>
            val event = parser.parse(context)
            builder.addEvent(event)
          }
        }
      }
    } catch {
      case e: Exception =>
        errorStatsAcc.add((s"${e.getClass.getName}", s"Error in $fileName: ${e.getMessage}"))
    }

    builder.build(fileName, sessionStart, sessionEnd)
  }

  private def getParserForLine(line: String): Option[EventParser] = {
    val parsers: Map[String, EventParser] = Map(
      "CARD_SEARCH_START" -> CardSearch,
      "QS" -> QuickSearch,
      "DOC_OPEN" -> DocumentOpen
    )

    parsers.keys.find(line.startsWith).map(parsers)
  }
}

package org.testtask.parser

import org.testtask.parser.events.utils.DateTimeParser
import org.testtask.parser.events.{CardSearch, DocumentOpen, Event, EventParser, QuickSearch}
import org.testtask.parser.processors.{ParsingContext, SessionBuilder}

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
    val builder = context.sessionBuilder

    try {
      while (lines.hasNext) {
        val line = lines.head
        val splitLine = line.split("\\s+")

        if (line.startsWith("SESSION_START")) {
          builder.sessionStart = DateTimeParser.parseTimestamp(splitLine.last, errorStatsAcc, fileName)
          lines.next()
        } else if (line.startsWith("SESSION_END")) {
          builder.sessionEnd = DateTimeParser.parseTimestamp(splitLine.last, errorStatsAcc, fileName)
          lines.next()
        } else {
          getParserForLine(line).foreach(_.parse(context))
        }
      }
    } catch {
      case e: Exception =>
        errorStatsAcc.add(
          (
            s"${e.getClass.getName}, ${e.getMessage}, ${e.getStackTrace.head}",
            s"Error in $fileName in line ${lines.head}"
          )
        )
    }

    builder.build()
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

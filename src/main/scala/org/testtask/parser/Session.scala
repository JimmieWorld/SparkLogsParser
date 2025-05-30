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
    allDocOpens: Seq[DocumentOpen]
)

object Session {

  private val allParsers = Seq(
    CardSearch,
    QuickSearch,
    DocumentOpen
  )

  private val parsers = allParsers.flatMap { parser =>
    parser.keys().map(key => key -> parser)
  }.toMap

  def extract(context: ParsingContext): Session = {
    while (context.lines.hasNext) {
      val line = context.lines.head
      try {
        val splitLine = line.split("\\s+")

        if (line.startsWith("SESSION_START")) {
          context.sessionBuilder.sessionStart = DateTimeParser.parseDateTime(splitLine.last, context)
          context.lines.next()
        } else if (line.startsWith("SESSION_END")) {
          context.sessionBuilder.sessionEnd = DateTimeParser.parseDateTime(splitLine.last, context)
          context.lines.next()
        } else {
          parsers.find { case (key, _) => line.startsWith(key) }.map(_._2).get.parse(context)
        }
      } catch {
        case e: Exception =>
          context.errorStats.add(
            (
              s"${e.getClass.getName}, ${e.getMessage}, ${e.getStackTrace.head}",
              s"Error in ${context.fileName} in line $line"
            )
          )
      }
    }

    context.sessionBuilder.build()
  }
}

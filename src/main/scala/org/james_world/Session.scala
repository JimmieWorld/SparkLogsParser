package org.james_world

import org.james_world.events.utils.{DateTimeParser, DocOpenLinker}
import org.james_world.events.{CardSearch, DocumentOpen, Event, EventParser, QuickSearch}

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
  def extract(context: ParsingContext, fileName: String): Session = {
    val bufferedIt = context.bufferedIt
    val errorStatsAcc = context.errorStatsAcc

    var cardSearches = Seq.empty[CardSearch]
    var quickSearches = Seq.empty[QuickSearch]
    var docOpens = Seq.empty[DocumentOpen]

    var sessionStart: Option[LocalDateTime] = None
    var sessionEnd: Option[LocalDateTime] = None

    try {
      while (bufferedIt.hasNext) {
        val line = bufferedIt.head
        val splitLine = line.split("\\s+")

        if (line.startsWith("SESSION_START")) {
          sessionStart = DateTimeParser.parseTimestamp(splitLine.last, errorStatsAcc)
          bufferedIt.next()
        } else if (line.startsWith("SESSION_END")) {
          sessionEnd = DateTimeParser.parseTimestamp(splitLine.last, errorStatsAcc)
          bufferedIt.next()
        } else {
          getParserForLine(line).foreach { parser =>
            val event = parser.parse(context)
            updateSearchTimestamp(event)

            event match {
              case cs: CardSearch    => cardSearches = cardSearches :+ cs
              case qs: QuickSearch   => quickSearches = quickSearches :+ qs
              case doo: DocumentOpen => docOpens = docOpens :+ doo
              case _                 => ()
            }
          }
        }
      }
    } catch {
      case e: Exception =>
        errorStatsAcc.add((s"${e.getClass.getName}", s"Error in $fileName: ${e.getMessage}"))
    }

    DocOpenLinker.linkDocOpensToSearches(
      Session(
        sessionId = fileName,
        sessionStart = sessionStart,
        sessionEnd = sessionEnd,
        cardSearches = cardSearches,
        quickSearches = quickSearches,
        docOpens = docOpens
      )
    )
  }

  private def getParserForLine(line: String): Option[EventParser] = {
    val parsers: Map[String, EventParser] = Map(
      "CARD_SEARCH" -> CardSearch,
      "QS" -> QuickSearch,
      "DOC_OPEN" -> DocumentOpen
    )

    parsers.keys.find(line.startsWith).map(parsers)
  }

  private def updateSearchTimestamp(event: Event): Unit = {
    event match {
      case qs: QuickSearch if qs.timestamp.isDefined =>
        val searchTimestamp = Map(qs.searchId -> qs.timestamp)
        DocumentOpen.addSearchTimestamp(searchTimestamp)
      case cs: CardSearch if cs.timestamp.isDefined =>
        val searchTimestamp = Map(cs.searchId -> cs.timestamp)
        DocumentOpen.addSearchTimestamp(searchTimestamp)
      case _ => ()
    }
  }
}

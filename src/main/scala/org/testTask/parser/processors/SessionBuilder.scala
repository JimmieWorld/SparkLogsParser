package org.testTask.parser.processors

import org.testTask.parser.Session
import org.testTask.parser.events.{CardSearch, DocumentOpen, Event, QuickSearch}

import java.time.LocalDateTime

case class SessionBuilder(
    context: ParsingContext
) {
  private var cardSearches = Seq.empty[CardSearch]
  private var quickSearches = Seq.empty[QuickSearch]
  private var docOpens = Seq.empty[DocumentOpen]

  def build(sessionId: String, sessionStart: Option[LocalDateTime], sessionEnd: Option[LocalDateTime]): Session = {
    val updatedQuickSearches = quickSearches.map { qs =>
      val matched = docOpens.filter(_.searchId == qs.searchId)
      qs.copy(docOpens = qs.docOpens ++ matched)
    }

    val updatedCardSearches = cardSearches.map { cs =>
      val matched = docOpens.filter(_.searchId == cs.searchId)
      cs.copy(docOpens = cs.docOpens ++ matched)
    }

    val unmatched = docOpens.filterNot(doo =>
      updatedQuickSearches.exists(_.searchId == doo.searchId) ||
        updatedCardSearches.exists(_.searchId == doo.searchId)
    )

    Session(
      sessionId = sessionId,
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = updatedCardSearches,
      quickSearches = updatedQuickSearches,
      docOpens = unmatched
    )
  }

  def addEvent(event: Event): Unit = event match {
    case qs: QuickSearch =>
      if (qs.timestamp.isDefined) {
        context.searchTimestamps += (qs.searchId -> qs.timestamp.get)
      }
      quickSearches :+= qs
    case cs: CardSearch =>
      if (cs.timestamp.isDefined) {
        context.searchTimestamps += (cs.searchId -> cs.timestamp.get)
      }
      cardSearches :+= cs
    case doo: DocumentOpen =>
      docOpens :+= doo
    case _ => ()
  }
}

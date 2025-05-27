package org.testtask.parser.processors

import org.testtask.parser.Session
import org.testtask.parser.events.{CardSearch, DocumentOpen, Event, QuickSearch}

import java.time.LocalDateTime

case class SessionBuilder(
    fileName: String,
    var sessionStart: Option[LocalDateTime] = None,
    var sessionEnd: Option[LocalDateTime] = None,
    var cardSearches: Seq[CardSearch] = Seq.empty,
    var quickSearches: Seq[QuickSearch] = Seq.empty,
    var docOpens: Seq[DocumentOpen] = Seq.empty
) {
  def build(): Session = {
    val enrichedDocOpens = enrichDocOpensWithTimestamp()

    val (updatedCardSearches, updatedQuickSearches, unmatchedDocOpens) = distributeDocOpens(enrichedDocOpens)

    Session(
      sessionId = fileName,
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = updatedCardSearches,
      quickSearches = updatedQuickSearches,
      docOpens = unmatchedDocOpens
    )
  }

  private def enrichDocOpensWithTimestamp(): Seq[DocumentOpen] = {
    val searchIdToTimestamp = (cardSearches.map(cs => cs.searchResult.searchId -> cs.timestamp) ++
      quickSearches.map(qs => qs.searchResult.searchId -> qs.timestamp))
      .collect { case (id, Some(ts)) => id -> ts}
      .toMap

    docOpens.foreach { doo =>
      if (doo.timestamp.isEmpty && searchIdToTimestamp.contains(doo.searchId)) {
        doo.timestamp = Some(searchIdToTimestamp(doo.searchId))
      }
    }
    docOpens
  }

  private def distributeDocOpens(
      enrichedDocOpens: Seq[DocumentOpen]
  ): (Seq[CardSearch], Seq[QuickSearch], Seq[DocumentOpen]) = {

    val docOpensGroupedBySearchId = enrichedDocOpens.groupBy(_.searchId)

    cardSearches.foreach { cs =>
      val matched = docOpensGroupedBySearchId.getOrElse(cs.searchResult.searchId, Nil)
      cs.searchResult.docOpens ++= matched
    }

    quickSearches.foreach { qs =>
      val matched = docOpensGroupedBySearchId.getOrElse(qs.searchResult.searchId, Nil)
      qs.searchResult.docOpens ++= matched
    }

    val unmatched = enrichedDocOpens.filterNot(doo => docOpensGroupedBySearchId.contains(doo.searchId))

    (cardSearches, quickSearches, unmatched)
  }
}

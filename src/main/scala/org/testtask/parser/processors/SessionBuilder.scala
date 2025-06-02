package org.testtask.parser.processors

import org.testtask.parser.Session
import org.testtask.parser.events.{CardSearch, DocumentOpen, QuickSearch}

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
    enrichDocOpensWithDateTime()
    distributeDocOpens()

    Session(
      sessionId = fileName,
      sessionStart = sessionStart,
      sessionEnd = sessionEnd,
      cardSearches = cardSearches,
      quickSearches = quickSearches,
      docOpens = docOpens
    )
  }

  private def enrichDocOpensWithDateTime(): Unit = {
    val searchId2DateTime = (
      cardSearches.map(cs => cs.searchResult.searchId -> cs.dateTime) ++
        quickSearches.map(qs => qs.searchResult.searchId -> qs.dateTime)
    ).collect { case (id, Some(dt)) => id -> dt }
    .toMap

    docOpens.foreach { docOpen =>
      if (docOpen.dateTime.isEmpty && searchId2DateTime.contains(docOpen.searchId)) {
        docOpen.dateTime = Some(searchId2DateTime(docOpen.searchId))
      }
    }
  }

  private def distributeDocOpens(): Unit = {

    val docOpensGroupedBySearchId = docOpens.groupBy(_.searchId)

    cardSearches.foreach { cs =>
      val matched = docOpensGroupedBySearchId.getOrElse(cs.searchResult.searchId, Nil)
      cs.searchResult.docOpens = matched
    }

    quickSearches.foreach { qs =>
      val matched = docOpensGroupedBySearchId.getOrElse(qs.searchResult.searchId, Nil)
      qs.searchResult.docOpens = matched
    }
  }
}

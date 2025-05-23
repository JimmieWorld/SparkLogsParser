package org.james_world.events.utils

import org.james_world.Session

object DocOpenLinker {
  def linkDocOpensToSearches(session: Session): Session = {
    val docOpenMap = session.docOpens.groupBy(_.searchId)

    val updatedQuickSearches = session.quickSearches.map { qs =>
      val matched = docOpenMap.getOrElse(qs.searchId, Nil)
      qs.copy(docOpens = qs.docOpens ++ matched)
    }

    val updatedCardSearches = session.cardSearches.map { cs =>
      val matched = docOpenMap.getOrElse(cs.searchId, Nil)
      cs.copy(docOpens = cs.docOpens ++ matched)
    }

    val unmatched = session.docOpens.filterNot(doc =>
      updatedQuickSearches.exists(_.searchId == doc.searchId) || updatedCardSearches.exists(_.searchId == doc.searchId)
    )

    session.copy(
      quickSearches = updatedQuickSearches,
      cardSearches = updatedCardSearches,
      docOpens = unmatched
    )
  }
}

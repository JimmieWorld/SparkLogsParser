package org.james_world

import org.james_world.Events.{
    CardSearchEvent, DocumentOpenEvent, QuickSearchEvent
}
import java.time.LocalDateTime

case class Session(
    sessionId: String,
    sessionStart: Option[LocalDateTime],
    sessionEnd: Option[LocalDateTime],
    cardSearches: Seq[CardSearchEvent],
    quickSearches: Seq[QuickSearchEvent],
    docOpens: Seq[DocumentOpenEvent]
)
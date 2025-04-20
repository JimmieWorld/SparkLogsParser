package org.james_world

import java.time.LocalDateTime

sealed trait Event {
    def timestamp: LocalDateTime
}

case class SessionStart(timestamp: LocalDateTime) extends Event
case class SessionEnd(timestamp: LocalDateTime) extends Event
case class QuickSearch(timestamp: LocalDateTime, searchId: String, queryText: String, relatedDocuments: Seq[String]) extends Event
case class DocumentOpen(timestamp: LocalDateTime, searchId: String, documentId: String) extends Event
case class CardSearch(timestamp: LocalDateTime, searchId: String, queriesTexts: Seq[String], relatedDocuments: Seq[String]) extends Event

case class Session(
    sessionId: String,
    events: Seq[Event]
)
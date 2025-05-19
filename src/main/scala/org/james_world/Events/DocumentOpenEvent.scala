package org.james_world.Events

import org.james_world.Extractors.{DateTimeExtractor, EventAttributesExtractors}
import java.time.LocalDateTime

case class DocumentOpenEvent(
    timestamp: LocalDateTime,
    searchId: String,
    documentId: String
) extends Event

object DocumentOpenEvent extends EventParser {
    private var searchTimestamps: Map[String, LocalDateTime] = Map.empty

    override def parse(lines: Seq[String]): Option[DocumentOpenEvent] = {
        if (lines.isEmpty) return None

        val searchId = EventAttributesExtractors.extractQueryId(lines.head)
        val documentId = EventAttributesExtractors.extractDocumentId(lines.head)
        val rawTimestamp = DateTimeExtractor.extractTimestamp(lines.head)
        val timestamp = if (rawTimestamp == LocalDateTime.of(0, 1, 1, 0, 0, 0)) {
            searchTimestamps.getOrElse(searchId, LocalDateTime.of(0, 1, 1, 0, 0, 0))
        } else {
            rawTimestamp
        }

        Some(DocumentOpenEvent(
            timestamp = timestamp,
            searchId = searchId,
            documentId = documentId
        ))
    }

    def addSearchTimestamp(timestamp: Map[String, LocalDateTime]): Unit = {
        searchTimestamps ++= timestamp
    }
}
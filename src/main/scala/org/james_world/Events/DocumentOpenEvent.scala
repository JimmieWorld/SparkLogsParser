package org.james_world.Events

import org.james_world.ErrorStatsAccumulator
import org.james_world.Extractors.{DateTimeExtractor, EventAttributesExtractors}

import java.time.LocalDateTime
import scala.collection.BufferedIterator

case class DocumentOpenEvent(
    timestamp: Option[LocalDateTime],
    searchId: String,
    documentId: String
) extends Event

object DocumentOpenEvent extends EventParser {
    private var searchTimestamps: Map[String, LocalDateTime] = Map.empty

    def clearSearchTimestamps(): Unit = {
        searchTimestamps = Map.empty
    }
    override def parse(
        bufferedIt: BufferedIterator[String],
        errorStatsAcc: ErrorStatsAccumulator
    ): Option[Event] = {
        if (!bufferedIt.hasNext) return None

        val line = bufferedIt.head
        val eventName = "DOC_OPEN"
        if (!line.startsWith(eventName)) return None

        val lineWithDoc = bufferedIt.next()
        val splitLineWithDoc = lineWithDoc.split("\\s+")

        val searchId = splitLineWithDoc(splitLineWithDoc.length - 2)
        val documentId = splitLineWithDoc.last

        val rawTimestamp = DateTimeExtractor.extractTimestamp(splitLineWithDoc(1), eventName, errorStatsAcc)
        val timestamp = if (rawTimestamp.isEmpty) {
            searchTimestamps.get(searchId)
        } else {
            rawTimestamp
        }

        Some(DocumentOpenEvent(
            timestamp = timestamp,
            searchId = searchId,
            documentId = documentId
        ))
    }

    def addSearchTimestamp(timestamp: Map[String, Option[LocalDateTime]]): Unit = {
        val validTimestamps = timestamp.collect {
            case (id, Some(dt)) => id -> dt  // берём только те, где dt определён
        }
        searchTimestamps ++= validTimestamps
    }
}
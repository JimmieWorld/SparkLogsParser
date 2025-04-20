package org.james_world

import java.io.File
import java.time.LocalDateTime
import scala.io.Source
import scala.util.Using

object LogParser {

    private val parsers: Map[String, EventParser] = Map(
        "SESSION_START" -> SessionStartParser,
        "CARD_SEARCH" -> CardSearchParser,
        "QS" -> QuickSearchParser,
        "DOC_OPEN" -> DocumentOpenParser,
        "SESSION_END" -> SessionEndParser
    )

    private def getParserForLine(line: String): Option[EventParser] = {
        parsers.keys.find(line.startsWith).map(parsers)
    }

    def parseLogFile(file: File): Session = {
        val fileName = file.getName
        val lines = readLines(file)
        processLines(lines, fileName)
    }

    /**
     * Метод для чтения строк из файла с обработкой ошибок.
     * @param file Файл для чтения.
     * @return Последовательность строк или пустая последовательность в случае ошибки.
     */
    private def readLines(file: File): Seq[String] = {
        Using(Source.fromFile(file, "Windows-1251")) { source =>
            source.getLines().toSeq
        }.getOrElse {
            println(s"Failed to read file: ${file.getName}")
            Seq.empty
        }
    }

    /**
     * Обработка строк лога и создание объекта Session.
     * @param lines Строки лога.
     * @param fileName Имя файла для идентификации сессии.
     * @return Объект Session.
     */
    private def processLines(lines: Seq[String], fileName: String): Session = {
        val firstLine = lines.headOption.map(_.trim).getOrElse("")

        if (firstLine.isEmpty) {
            return Session(sessionId = fileName, events = Seq.empty)
        }

        lines.tail.foldLeft((Seq.empty[Event], Seq(lines.head))) { case ((events, buffer), line) =>
            val trimmedLine = line.trim
            val updatedBuffer = buffer :+ trimmedLine

            if (isNextEvent(trimmedLine)) {
                val newEvent = getParserForLine(buffer.head).flatMap(_.parse(buffer)).toSeq
                newEvent.foreach(updateSearchTimestamp)
                (events ++ newEvent, Seq(trimmedLine))
            } else {
                (events, updatedBuffer)
            }
        } match {
            case (events, buffer) =>
                val finalEvents = if (buffer.nonEmpty) {
                    getParserForLine(buffer.head).flatMap(_.parse(buffer)).toSeq
                } else {
                    Seq.empty
                }
                val allEvents = events ++ finalEvents
                val uniqueEvents = removeDuplicateEvents(allEvents)

                Session(sessionId = fileName, events = uniqueEvents)
        }
    }

    private def updateSearchTimestamp(event: Event): Unit = {
        event match {
            case qs: QuickSearch if qs.timestamp != LocalDateTime.of(0, 1, 1, 0, 0, 0) =>
                val searchTimestamp = Map(qs.searchId -> qs.timestamp)
                DocumentOpenParser.addSearchTimestamp(searchTimestamp)
            case cs: CardSearch if cs.timestamp != LocalDateTime.of(0, 1, 1, 0, 0, 0) =>
                val searchTimestamp = Map(cs.searchId -> cs.timestamp)
                DocumentOpenParser.addSearchTimestamp(searchTimestamp)
            case _ => ()
        }
    }

    /**
     * Удаляет полные дубликаты событий.
     * @param events Список событий.
     * @return Список уникальных событий.
     */
    private def removeDuplicateEvents(events: Seq[Event]): Seq[Event] = {
        events
            .groupBy(identity)
            .values
            .map(_.head)
            .toSeq
    }

    /**
     * Проверяет, является ли строка началом нового события.
     * @param line Строка для проверки.
     * @return `true`, если строка начинается с ключевого слова нового события.
     */
    private def isNextEvent(line: String): Boolean = {
        line.startsWith("DOC_OPEN") ||
        line.startsWith("CARD_SEARCH_START") ||
        line.startsWith("QS") ||
        line.startsWith("SESSION_END") ||
        line.startsWith("SESSION_START")
    }

    /**
     * Метод для структурированного вывода объекта Session в консоль.
     * @param session Объект Session для вывода.
     */
    def printSession(session: Session): Unit = {
        println(s"Session ID: ${session.sessionId}")
        println("Events:")

        session.events.foreach {
            case SessionStart(timestamp) =>
                println(s"  [SessionStart] Timestamp: $timestamp")
            case SessionEnd(timestamp) =>
                println(s"  [SessionEnd] Timestamp: $timestamp")
            case QuickSearch(timestamp, searchId, queryText, relatedDocuments) =>
                println(s"  [QuickSearch] Timestamp: $timestamp, Search ID: $searchId")
                println(s"    Query Text: $queryText")
                println(s"    Related Documents: ${relatedDocuments.mkString(", ")}")
            case DocumentOpen(timestamp, searchId, documentId) =>
                println(s"  [DocumentOpen] Timestamp: $timestamp, Search ID: $searchId")
                println(s"    Document ID: $documentId")
            case CardSearch(timestamp, searchId, queriesTexts, relatedDocuments) =>
                println(s"  [CardSearch] Timestamp: $timestamp, Search ID: $searchId")
                println(s"    Queries: ${queriesTexts.mkString(", ")}")
                println(s"    Related Documents: ${relatedDocuments.mkString(", ")}")
            case _ =>
                println(s"  [Unknown Event]")
        }
    }
}
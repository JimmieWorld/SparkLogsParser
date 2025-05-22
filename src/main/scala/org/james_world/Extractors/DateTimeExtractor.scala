package org.james_world.Extractors

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.james_world.ErrorStatsAccumulator

object DateTimeExtractor {

    private val defaultFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
    private val quickSearchFormatters = List(
        DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss", Locale.ENGLISH),
        defaultFormat
    )
    private val cardSearchFormatters = quickSearchFormatters

    def extractTimestamp(
        line: String,
        eventType: String,
        errorStatsAcc: ErrorStatsAccumulator
    ): Option[LocalDateTime] = {
        val dateString = line
            .trim
            .replaceAll("(_[+-]\\d{4})$", "")
            .trim

        val formatters = eventType match {
            case "SESSION_START" => List(defaultFormat)
            case "QS" => quickSearchFormatters
            case "CARD_SEARCH_START" => cardSearchFormatters
            case "DOC_OPEN" => List(defaultFormat)
            case "SESSION_END" => List(defaultFormat)
            case _ => List.empty
        }

        val result = formatters.view.flatMap { formatter =>
            try {
                Some(LocalDateTime.parse(dateString, formatter))
            } catch {
                case _: Exception => None
            }
        }.headOption

        result match {
            case Some(dt) => Some(dt)
            case None =>
                errorStatsAcc.add((
                    "InvalidTimestampFormat",
                    s"[$eventType] Failed to parse timestamp from line: $line"
                ))
                None
        }
    }
}
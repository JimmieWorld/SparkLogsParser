package org.testtask.parser.events.utils

import org.testtask.parser.processors.ParsingContext

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

object DateTimeParser {

  private val format1 = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val format2 = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z", Locale.ENGLISH)

  private val targetZone: ZoneId = ZoneId.of("Europe/Moscow")

  def parseDateTime(
      line: String,
      context: ParsingContext
  ): Option[LocalDateTime] = {

    Try(LocalDateTime.parse(line, format1)).toOption.foreach { dateTime =>
      return Some(dateTime)
    }

    Try(ZonedDateTime.parse(line, format2)).toOption.foreach { zonedDateTime =>
      val dateTime = zonedDateTime.withZoneSameInstant(targetZone).toLocalDateTime
      return Some(dateTime)
    }

    context.errorStats.add(
      (
        "Warning: InvalidDateTimeFormat",
        s"[file ${context.fileName}] Failed to parse dateTime from line: $line"
      )
    )
    None
  }
}

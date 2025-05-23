package org.james_world.events.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.james_world.ErrorStatsAccumulator

import scala.util.Try

object DateTimeParser {

  private val defaultFormat = DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")
  private val secondaryFormat = DateTimeFormatter.ofPattern("EEE,_d_MMM_yyyy_HH:mm:ss", Locale.ENGLISH)

  def parseTimestamp(
      line: String,
      errorStatsAcc: ErrorStatsAccumulator
  ): Option[LocalDateTime] = {
    val dateString = line.trim
      .replaceAll("(_[+-]\\d{4})$", "")
      .trim

    if (Try(LocalDateTime.parse(dateString, defaultFormat)).isSuccess) {
      Some(LocalDateTime.parse(dateString, defaultFormat))
    } else if (Try(LocalDateTime.parse(dateString, secondaryFormat)).isSuccess) {
      Some(LocalDateTime.parse(dateString, secondaryFormat))
    } else {
      errorStatsAcc.add(
        (
          "InvalidTimestampFormat",
          s"Failed to parse timestamp from line: $line"
        )
      )
      None
    }
  }
}

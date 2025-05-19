import org.james_world.Extractors.DateTimeExtractor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime

class TestDateTimeExtractor extends AnyFlatSpec with Matchers {

    "DateTimeExtractor.extractTimestamp" should "parse valid date in EEE,_d_MMM_yyyy_HH:mm:ss_Z format" in {
        val line = "Thu,_5_Oct_2023_14:30:00_+0300"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2023, 10, 5, 14, 30, 0)
    }

    it should "parse valid date in EEE, d MMM yyyy HH:mm:ss Z format" in {
        val line = "Thu, 5 Oct 2023 14:30:00 +0300"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2023, 10, 5, 14, 30, 0)
    }

    it should "parse 'Some_Text 23.07.2020_22:03:45'" in {
        val line = "CARD_SEARCH_START 23.07.2020_22:03:45"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2020, 7, 23, 22, 3, 45)
    }

    it should "parse 'Some Text 23.07.2020_22:03:45'" in {
        val line = "Some Text 23.07.2020_22:03:45"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2020, 7, 23, 22, 3, 45)
    }

    it should "parse valid date in dd.MM.yyyy_HH:mm:ss format" in {
        val line = "05.10.2023_14:30:00"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2023, 10, 5, 14, 30, 0)
    }

    it should "parse valid date in yyyy-MM-dd HH:mm:ss format" in {
        val line = "2023-10-05 14:30:00"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2023, 10, 5, 14, 30, 0)
    }

    it should "parse valid date in dd/MM/yyyy HH:mm:ss format" in {
        val line = "05/10/2023 14:30:00"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2023, 10, 5, 14, 30, 0)
    }

    it should "parse valid date in dd.MM.yy HH:mm:ss format" in {
        val line = "05.10.23 14:30:00"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2023, 10, 5, 14, 30, 0)
    }

    it should "return default (year 0) for empty string" in {
        val result = DateTimeExtractor.extractTimestamp("")
        result shouldEqual LocalDateTime.of(0, 1, 1, 0, 0, 0)
    }

    it should "return default for garbage input" in {
        val line = "This is not a date at all!"
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(0, 1, 1, 0, 0, 0)
    }

    it should "trim whitespace before parsing" in {
        val line = "   2023-10-05 14:30:00   "
        val result = DateTimeExtractor.extractTimestamp(line)
        result shouldEqual LocalDateTime.of(2023, 10, 5, 14, 30, 0)
    }
}
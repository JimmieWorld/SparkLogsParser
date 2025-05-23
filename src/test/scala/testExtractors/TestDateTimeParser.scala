package testExtractors

import org.james_world.ErrorStatsAccumulator
import org.james_world.events.utils.DateTimeParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach
import java.time.LocalDateTime

class TestDateTimeParser extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

    var errorStatsAcc: ErrorStatsAccumulator = _

    override def beforeEach(): Unit = {
        errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
        super.beforeEach()
    }

    "extractTimestamp" should "parse default format (dd.MM.yyyy_HH:mm:ss)" in {
        val line = "13.02.2020_21:37:23"
        val result = DateTimeParser.parseTimestamp(line, errorStatsAcc)

        result shouldBe defined
        result.get shouldEqual LocalDateTime.of(2020, 2, 13, 21, 37, 23)
        verifyZeroInteractions(errorStatsAcc)
    }

    it should "parse RFC822-like format for QS event" in {
        val line = "Thu,_13_Feb_2020_21:38:09_+0300"
        val result = DateTimeParser.parseTimestamp(line, errorStatsAcc)

        result shouldBe defined
        result.get shouldEqual LocalDateTime.of(2020, 2, 13, 21, 38, 9)
        verifyZeroInteractions(errorStatsAcc)
    }

    it should "fallback to default format for QS if RFC fails" in {
        val line = "13.02.2020_21:38:09"
        val result = DateTimeParser.parseTimestamp(line, errorStatsAcc)

        result shouldBe defined
        result.get shouldEqual LocalDateTime.of(2020, 2, 13, 21, 38, 9)
        verifyZeroInteractions(errorStatsAcc)
    }

    it should "parse timestamp for CARD_SEARCH_START" in {
        val line = "Thu,_13_Feb_2020_21:59:25_+0300"
        val result = DateTimeParser.parseTimestamp(line, errorStatsAcc)

        result shouldBe defined
        result.get shouldEqual LocalDateTime.of(2020, 2, 13, 21, 59, 25)
        verifyZeroInteractions(errorStatsAcc)
    }

    it should "fail on invalid date and log error" in {
        val line = "BAD_DATE_FORMAT"
        val result = DateTimeParser.parseTimestamp(line, errorStatsAcc)

        result shouldBe None
        verify(errorStatsAcc).add((
            "InvalidTimestampFormat",
            "[QS] Failed to parse timestamp from line: BAD_DATE_FORMAT"
        ))
    }

    it should "parse DOC_OPEN timestamps using default format" in {
        val line = "13.02.2020_21:45:55"
        val result = DateTimeParser.parseTimestamp(line, errorStatsAcc)

        result shouldBe defined
        result.get shouldEqual LocalDateTime.of(2020, 2, 13, 21, 45, 55)
        verifyZeroInteractions(errorStatsAcc)
    }

    it should "return None if no formatter can parse the date" in {
        val line = "INVALID_DATE_STRING"
        val result = DateTimeParser.parseTimestamp(line, errorStatsAcc)

        result shouldBe None
        verify(errorStatsAcc).add((
            "InvalidTimestampFormat",
            "[SESSION_START] Failed to parse timestamp from line: INVALID_DATE_STRING"
        ))
    }
}
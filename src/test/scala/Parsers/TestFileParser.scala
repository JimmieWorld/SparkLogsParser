package Parsers

import org.james_world.ErrorStatsAccumulator
import org.james_world.Parsers.TextFileParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.Mockito._
import org.mockito.MockitoSugar.verifyZeroInteractions
import org.scalatest.BeforeAndAfterEach
import java.time.LocalDateTime

class TestFileParser extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

    var errorStatsAcc: ErrorStatsAccumulator = _

    override def beforeEach(): Unit = {
        errorStatsAcc = mock(classOf[ErrorStatsAccumulator])
        super.beforeEach()
    }

    "TextFileParser.processLines" should "parse a full valid session with multiple events" in {
        val lines = List(
            "SESSION_START 13.02.2020_21:37:23",
            "QS Thu,_13_Feb_2020_21:38:09_+0300 {пенсия работающим пенсионерам}",
            "-981704369 PSR_70597 PBO_22363",
            "DOC_OPEN 13.02.2020_21:45:55 -981704369 PSR_70597",
            "SESSION_END 13.02.2020_22:07:57"
        )

        val session = TextFileParser.processLines(lines, "1", errorStatsAcc)

        session.sessionId shouldBe "1"
        session.sessionStart shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 37, 23))
        session.sessionEnd shouldBe Some(LocalDateTime.of(2020, 2, 13, 22, 7, 57))

        session.quickSearches should have size 1
        session.docOpens should have size 1
        session.cardSearches should be(empty)

        verifyZeroInteractions(errorStatsAcc)
    }

    it should "log error when parser not found" in {
        val lines = List(
            "SESSION_START 13.02.2020_21:37:23",
            "UNKNOWN_EVENT_TYPE some content here"
        )

        val session = TextFileParser.processLines(lines, "2", errorStatsAcc)

        session.sessionStart.map(_.toLocalDate) shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 37, 23).toLocalDate)
        session.cardSearches shouldBe empty
        session.quickSearches shouldBe empty
        session.docOpens shouldBe empty

        verify(errorStatsAcc).add((
            "NoParserFound",
            "[2] No parser found for line: UNKNOWN_EVENT_TYPE some content here"
        ))
    }

    it should "parse QS and DOC_OPEN correctly using same searchId" in {
        val lines = List(
            "SESSION_START 13.02.2020_21:37:23",
            "QS 13.02.2020_21:38:09 {query}",
            "-1234 DOC1 DOC2",
            "DOC_OPEN 13.02.2020_21:39:00 -1234 DOC1",
            "SESSION_END 13.02.2020_22:07:57"
        )

        val session = TextFileParser.processLines(lines, "3", errorStatsAcc)

        session.quickSearches should have size 1
        session.docOpens should have size 1

        session.quickSearches.head.searchId shouldBe "-1234"
        session.docOpens.head.searchId shouldBe "-1234"

        verifyZeroInteractions(errorStatsAcc)
    }

    it should "parse mixed event types correctly" in {
        val lines = List(
            "SESSION_START 13.02.2020_21:37:23",
            "QS 13.02.2020_21:38:09 {query}",
            "-1234 DOC1 DOC2",
            "DOC_OPEN 13.02.2020_21:39:00 -1234 DOC1",
            "SESSION_END 13.02.2020_22:07:57"
        )

        val session = TextFileParser.processLines(lines, "4", errorStatsAcc)

        session.sessionStart.map(_.getHour) shouldBe Some(21)
        session.sessionEnd.map(_.getHour) shouldBe Some(22)
        session.quickSearches should have size 1
        session.docOpens should have size 1

        session.docOpens.head.documentId shouldBe "DOC1"
        session.quickSearches.head.relatedDocuments should contain theSameElementsAs Seq("DOC1", "DOC2")
    }

    it should "skip invalid lines and continue parsing" in {
        val lines = List(
            "SOME_UNKNOWN_LINE",
            "SESSION_START 13.02.2020_21:37:23",
            "SESSION_END 13.02.2020_22:07:57"
        )

        val session = TextFileParser.processLines(lines, "5", errorStatsAcc)

        session.sessionStart.map(_.toLocalDate) shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 37, 23).toLocalDate)
        session.sessionEnd.map(_.toLocalDate) shouldBe Some(LocalDateTime.of(2020, 2, 13, 22, 7, 57).toLocalDate)

        verify(errorStatsAcc).add(("NoParserFound", "[5] No parser found for line: SOME_UNKNOWN_LINE"))
    }

    it should "use timestamp from search if not in line" in {
        val lines = List(
            "SESSION_START 13.02.2020_21:37:23",
            "QS 13.02.2020_21:38:09 {query}",
            "-1234 DOC1 DOC2",
            "DOC_OPEN  -1234 DOC1",
            "SESSION_END 13.02.2020_22:07:57"
        )

        val session = TextFileParser.processLines(lines, "6", errorStatsAcc)

        session.docOpens should have size 1
        val docOpen = session.docOpens.head

        docOpen.searchId shouldBe "-1234"
        docOpen.documentId shouldBe "DOC1"
        docOpen.timestamp shouldBe Some(LocalDateTime.of(2020, 2, 13, 21, 38, 9))

        errorStatsAcc.value shouldBe null
    }
}
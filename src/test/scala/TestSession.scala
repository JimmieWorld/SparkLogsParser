import org.james_world.Events.{CardSearchEvent, DocumentOpenEvent, QuickSearchEvent}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import org.james_world.Session

class TestSession extends AnyFlatSpec with Matchers {

    "Session.processLines" should "parse SessionStartEvent and SessionEndEvent correctly" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "SESSION_END 2023-10-01T11:00:00"
        )

        val session = Session.processLines(lines, "session1")

        session.sessionId shouldBe "session1"
        session.events should have size 2
        session.events.map(_.getClass.getSimpleName) shouldBe Seq("SessionStartEvent", "SessionEndEvent")
    }

    it should "parse CardSearchEvent with related documents" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "CARD_SEARCH_START 2023-10-01_12:05:20",
            "$134 131-фз",
            "CARD_SEARCH_END",
            "4321 DOC_1 DOC_2 DOC_3"
        )

        val session = Session.processLines(lines, "session2")

        session.events should have size 2
        session.events.last shouldBe a[CardSearchEvent]
    }

    it should "correctly parse all event types in sequence" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "CARD_SEARCH_START 2023-10-01_12:05:20",
            "$134 131-фз",
            "CARD_SEARCH_END",
            "54321 DOC_1 DOC_2 DOC_3",
            "DOC_OPEN 54321 DOC_1",
            "SESSION_END 2023-10-01T11:00:00"
        )

        val session = Session.processLines(lines, "session3")

        session.events should have size 4
        session.events.map(_.getClass.getSimpleName) shouldBe Seq(
            "SessionStartEvent",
            "CardSearchEvent",
            "DocumentOpenEvent",
            "SessionEndEvent"
        )
    }

    it should "update searchTimestamp when parsing CardSearchEvent" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "CARD_SEARCH_START 2023-10-01_12:05:20",
            "$134 131-фз",
            "CARD_SEARCH_END",
            "6812002 DOC_1 DOC_2",
            "DOC_OPEN  6812002 DOC_2"
        )

        val session = Session.processLines(lines, "session4")

        println(session)

        session.events should have size 3
        session.events.last.timestamp shouldEqual LocalDateTime.of(2023, 10, 1, 12, 5, 20)
    }

    it should "ignore duplicate events across multiple sessions" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "CARD_SEARCH_START 2023-10-01_12:05:20",
            "$134 131-фз",
            "CARD_SEARCH_END",
            "6812002 DOC_1 DOC_2",
            "CARD_SEARCH_START 2023-10-01_12:05:20",
            "$134 131-фз",
            "CARD_SEARCH_END",
            "6812002 DOC_1 DOC_2",
            "SESSION_END 2023-10-01T11:00:00"
        )

        val session = Session.processLines(lines, "session5")

        session.events.count(_.isInstanceOf[CardSearchEvent]) shouldBe 1
    }

    it should "create session with sessionId from filename" in {
        val lines = Seq("SESSION_START 2023-10-01T10:00:00")
        val session = Session.processLines(lines, "file_session6")

        session.sessionId shouldBe "file_session6"
    }

    it should "handle malformed line but still process valid ones" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "invalid_line_that_does_not_match_any_parser",
            "SESSION_END 2023-10-01T11:00:00"
        )

        val session = Session.processLines(lines, "session7")

        session.events should have size 2
        session.events.map(_.getClass.getSimpleName) shouldBe Seq("SessionStartEvent", "SessionEndEvent")
    }

    it should "parse mixed events and preserve order" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "QS 2023-10-01T10:01:00 {query1}",
            "123456 DOC_1 DOC_2",
            "DOC_OPEN 123456 DOC_1",
            "SESSION_END 2023-10-01T11:00:00"
        )

        val session = Session.processLines(lines, "session8")

        session.events.map(_.getClass.getSimpleName) shouldBe Seq(
            "SessionStartEvent",
            "QuickSearchEvent",
            "DocumentOpenEvent",
            "SessionEndEvent"
        )
    }

    it should "not add empty events" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "",
            "SESSION_END 2023-10-01T11:00:00"
        )

        val session = Session.processLines(lines, "session9")

        session.events should have size 2
        session.events.map(_.getClass.getSimpleName) shouldBe Seq("SessionStartEvent", "SessionEndEvent")
    }

    it should "parse multiple events of different types" in {
        val lines = Seq(
            "SESSION_START 2023-10-01T10:00:00",
            "QS 2023-10-01T10:01:00 {query1}",
            "1234567 DOC_1 DOC_2",
            "DOC_OPEN 1234567 DOC_1",
            "CARD_SEARCH_START 2023-10-01_12:05:20",
            "$134 131-фз",
            "CARD_SEARCH_END",
            "6812002 DOC_1 DOC_3",
            "DOC_OPEN 6812002 DOC_1",
            "SESSION_END 2023-10-01T11:00:00"
        )

        val session = Session.processLines(lines, "session10")

        session.events.map(_.getClass.getSimpleName) shouldBe Seq(
            "SessionStartEvent",
            "QuickSearchEvent",
            "DocumentOpenEvent",
            "CardSearchEvent",
            "DocumentOpenEvent",
            "SessionEndEvent"
        )
    }
}
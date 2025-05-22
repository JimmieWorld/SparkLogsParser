import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.{LocalDate, LocalDateTime}
import org.james_world.Events._
import org.james_world.{Main, Session}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Ignore

@Ignore
trait TestSparkContext extends AnyFlatSpec with Matchers with BeforeAndAfterAll  {
    private var _sc: SparkContext = _

    def sc: SparkContext = _sc

    override def beforeAll(): Unit = {
        val conf = new SparkConf().setAppName("test").setMaster("local[*]")
        _sc = new SparkContext(conf)
    }

    override def afterAll(): Unit = {
        if (_sc != null) _sc.stop()
    }
}

class TestCountDocumentOpens extends AnyFlatSpec with Matchers with TestSparkContext {

    val sessionStart: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 0))
    val sessionEnd: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 5))

    "countDocumentOpens" should "correctly count document opens after quick search" in {
        val session = Session(
            sessionId = "session1",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq.empty,
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
                    searchId = "s1",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616", "DOC_123")
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "s1",
                    documentId = "ACC_45616"
                ),
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                    searchId = "s1",
                    documentId = "ACC_45616"
                ),
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
                    searchId = "s2",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session)))

        val expected = Seq(
            (LocalDate.of(2023, 8, 1), "ACC_45616",  2)
        )
        result shouldBe expected
    }

    it should "not count document opens with quick search" in {
        val session = Session(
            sessionId = "session2",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "cs1",
                    queriesTexts = Seq("$ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            quickSearches = Seq.empty,
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "cs1",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session)))
        result shouldBe Seq.empty
    }

    it should "not count document opens without quick search" in {
        val session = Session(
            sessionId = "session2",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "cs1",
                    queriesTexts = Seq("$ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "qs1",
                    queryText = "",
                    relatedDocuments = Seq()
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "cs1",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session)))
        result shouldBe empty
    }

    it should "count document opens across multiple sessions" in {
        val session1 = Session(
            sessionId = "session1",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq.empty,
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
                    searchId = "s1",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "s1",
                    documentId = "ACC_45616"
                )
            )
        )

        val session2 = Session(
            sessionId = "session2",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq.empty,
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 13, 0)),
                    searchId = "s2",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 13, 5)),
                    searchId = "s2",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session1, session2)))

        val expected = Seq(
            (LocalDate.of(2023, 8, 1), "ACC_45616", 2)
        )

        result shouldBe expected
    }

    it should "count document opens on different dates and sessions separately" in {
        val session1 = Session(
            sessionId = "session1",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq.empty,
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
                    searchId = "s1",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "s1",
                    documentId = "ACC_45616"
                )
            )
        )

        val session2 = Session(
            sessionId = "session2",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq.empty,
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 2, 12, 0)),
                    searchId = "s2",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 2, 12, 5)),
                    searchId = "s2",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session1, session2)))

        val expected = Seq(
            (LocalDate.of(2023, 8, 1), "ACC_45616", 1),
            (LocalDate.of(2023, 8, 2), "ACC_45616", 1)
        )

        result.toSet shouldBe expected.toSet
    }

    it should "count document opened after both QS and CS" in {
        val session = Session(
            sessionId = "session1",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "cs1",
                    queriesTexts = Seq("$ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
                    searchId = "qs1",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                    searchId = "qs1",
                    documentId = "ACC_45616"
                ),
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
                    searchId = "cs1",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session)))

        val expected = Seq(
            (LocalDate.of(2023, 8, 1), "ACC_45616", 1)
        )

        result shouldBe expected
    }

    it should "count document opened after multiple QS" in {
        val session = Session(
            sessionId = "session2",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq.empty,
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
                    searchId = "qs1",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                ),
                QuickSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "qs2",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                    searchId = "qs1",
                    documentId = "ACC_45616"
                ),
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
                    searchId = "qs2",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session)))

        val expected = Seq(
            (LocalDate.of(2023, 8, 1), "ACC_45616", 2)
        )

        result shouldBe expected
    }

    it should "count document opened after multiple CS" in {
        val session = Session(
            sessionId = "session3",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
                    searchId = "cs1",
                    queriesTexts = Seq("$ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                ),
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "cs2",
                    queriesTexts = Seq("$ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            quickSearches = Seq.empty,
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 10)),
                    searchId = "cs1",
                    documentId = "ACC_45616"
                ),
                DocumentOpenEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 15)),
                    searchId = "cs2",
                    documentId = "ACC_45616"
                )
            )
        )

        val result = Main.task2(sc.parallelize(Seq(session)))

        val expected = Seq.empty

        result shouldBe expected
    }
}

class TestCountSearchesForDocument extends AnyFlatSpec with Matchers with TestSparkContext {

    val sessionStart: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 0))
    val sessionEnd: Option[LocalDateTime] = Some(LocalDateTime.of(2023, 8, 1, 12, 5))

    "countSearchesForDocument" should "count only matching CardSearchEvents in multiple sessions" in {
        val session1 = Session(
            sessionId = "session1",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 0)),
                    searchId = "cs1",
                    queriesTexts = Seq("ACC_45617"),
                    relatedDocuments = Seq.empty
                ),
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 1, 12, 5)),
                    searchId = "cs2",
                    queriesTexts = Seq("ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            quickSearches = Seq.empty,
            docOpens = Seq.empty
        )

        val session2 = Session(
            sessionId = "session2",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(LocalDateTime.of(2023, 8, 2, 10, 0)),
                    searchId = "cs3",
                    queriesTexts = Seq("DOC_789"),
                    relatedDocuments = Seq("DOC_789")
                )
            ),
            quickSearches = Seq.empty,
            docOpens = Seq.empty
        )

        val result = Main.task1(sc.parallelize(Seq(session1, session2)), "ACC_45616")

        result shouldBe 1
    }

    it should "return zero if no matching documents found" in {
        val session = Session(
            sessionId = "session1",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(sessionStart.get.plusMinutes(2)),
                    searchId = "cs1",
                    queriesTexts = Seq("DOC_123", "DOC_456"),
                    relatedDocuments = Seq("DOC_123")
                )
            ),
            quickSearches = Seq.empty,
            docOpens = Seq(
                DocumentOpenEvent(
                    timestamp = Some(sessionStart.get.plusMinutes(3)),
                    searchId = "qs1",
                    documentId = "DOC_123"
                )
            )
        )

        val result = Main.task1(sc.parallelize(Seq(session)), "ACC_45616")
        result shouldBe 0
    }

    it should "ignore unrelated query texts and other event types" in {
        val session = Session(
            sessionId = "session3",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(sessionStart.get.plusMinutes(2)),
                    searchId = "cs1",
                    queriesTexts = Seq("NOT_ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            quickSearches = Seq(
                QuickSearchEvent(
                    timestamp = Some(sessionStart.get.plusMinutes(3)),
                    searchId = "qs1",
                    queryText = "ACC_45616",
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            docOpens = Seq.empty
        )

        val result = Main.task1(sc.parallelize(Seq(session)), "ACC_45616")
        result shouldBe 0
    }

    it should "count correctly across multiple sessions" in {
        val session1 = Session(
            sessionId = "session1",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(sessionStart.get.plusMinutes(2)),
                    searchId = "cs1",
                    queriesTexts = Seq("ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                )
            ),
            quickSearches = Seq.empty,
            docOpens = Seq.empty
        )

        val session2 = Session(
            sessionId = "session2",
            sessionStart = sessionStart,
            sessionEnd = sessionEnd,
            cardSearches = Seq(
                CardSearchEvent(
                    timestamp = Some(sessionStart.get.plusMinutes(3)),
                    searchId = "cs2",
                    queriesTexts = Seq("ACC_45616"),
                    relatedDocuments = Seq("ACC_45616")
                ),
                CardSearchEvent(
                    timestamp = Some(sessionStart.get.plusMinutes(3)),
                    searchId = "cs3",
                    queriesTexts = Seq("ACC_45616", "DOC_123"),
                    relatedDocuments = Seq("DOC_123")
                )
            ),
            quickSearches = Seq.empty,
            docOpens = Seq.empty
        )

        val result = Main.task1(sc.parallelize(Seq(session1, session2)), "ACC_45616")
        result shouldBe 3
    }
}
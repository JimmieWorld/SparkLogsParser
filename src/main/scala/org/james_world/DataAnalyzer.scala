package org.james_world

import java.io.File
import org.apache.spark.rdd.RDD
import java.time.LocalDate

trait DataAnalyzer {
    def processFile(file: File): Seq[_]

    def analyze(filesRDD: RDD[File]): Map[_, _] = {
        def processFileWrapper(file: File): Seq[_] = {
            try {
                processFile(file)
            } catch {
                case e: Exception =>
                    Seq.empty
            }
        }
        filesRDD
            .flatMap(processFileWrapper)
            .groupBy(identity)
            .mapValues(_.size)
            .collectAsMap()
            .toMap
    }
}

class DocumentSearchAnalyzer(documentId: String) extends DataAnalyzer {
    override def processFile(file: File): Seq[(String, Long)] = {
        val session = LogParser.parseLogFile(file)
        SessionProcessor.countSearchesForDocument(session, documentId)
    }

    override def analyze(filesRDD: RDD[File]): Map[String, Long] = {
        super.analyze(filesRDD).asInstanceOf[Map[String, Long]]
    }
}

class DocumentOpenAnalyzer extends DataAnalyzer {
    override def processFile(file: File): Seq[((LocalDate, String), Int)] = {
        val session = LogParser.parseLogFile(file)
        SessionProcessor.countDocumentOpens(session)
    }

    override def analyze(filesRDD: RDD[File]): Map[(LocalDate, String), Int] = {
        super.analyze(filesRDD).asInstanceOf[Map[(LocalDate, String), Int]]
    }
}
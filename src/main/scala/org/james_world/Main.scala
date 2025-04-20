package org.james_world

import org.apache.spark.{SparkConf, SparkContext}

import java.io.File
//import java.time.LocalDate

object Main {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ConsultantPlusLogs").setMaster("local[*]")
        val sc = new SparkContext(conf)

        val filePath = "src/main/resources/data"
        val files = new File(filePath).listFiles()
        val filesRDD = sc.parallelize(files)

        val searchAnalyzer = new DocumentSearchAnalyzer("ACC_45616")
        val openAnalyzer = new DocumentOpenAnalyzer
        val searchResults = searchAnalyzer.analyze(filesRDD)
        val openResults = openAnalyzer.analyze(filesRDD)
        val FileSaver = new FileSaver()

        println(searchResults)
        FileSaver.saveResultsToCSV(openResults, "data\\results\\open_results.csv")

//        val aggregatedResult1 = filesRDD
//            .flatMap { file =>
//                try {
//                    val session = LogParser.parseLogFile(file)
//                    SessionProcessor.countSearchesForDocument(session, "ACC_45616")
//                } catch {
//                    case e: Exception =>
//                        Seq.empty[(String, Long)]
//                }
//            }
//            .reduceByKey(_ + _)
//            .collectAsMap()
//
//        val aggregatedResult2 = filesRDD
//            .flatMap { file =>
//                try {
//                    val session = LogParser.parseLogFile(file)
//                    SessionProcessor.countDocumentOpens(session)
//                } catch {
//                    case e: Exception =>
//                        Seq.empty[((LocalDate, String), Int)]
//                }
//            }
//            .reduceByKey(_ + _)
//            .collectAsMap()
//            .toMap
//        val FileSaver = new FileSaver()
//        FileSaver.saveResultsToCSV(aggregatedResult2, "src/main/resources/results/open_results.csv")

//        val aggregatedResult = files
//            .flatMap { file =>
//                val session = LogParser.parseLogFile(file)
//                SessionProcessor.countSearchesForDocument(session, "ACC_45616")
//            }.groupBy(_._1)
//            .map { case (documentId, counts) =>
//                val total = counts.map(_._2).sum
//                (documentId, total)
//            }

        sc.stop()
    }
}
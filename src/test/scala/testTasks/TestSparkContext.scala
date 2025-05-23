package testTasks

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

@Ignore
trait TestSparkContext extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
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

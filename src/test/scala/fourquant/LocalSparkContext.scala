package fourquant

/**
 * Based on the implementation from the Spark Testing Suite, a set of commands to initialize a
 * Spark context for tests
 * Created by mader on 10/10/14.
 */

import _root_.io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.log4j.Logger
import org.apache.log4j.varia.NullAppender
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll {

  self: Suite =>

  @transient private var isc: Option[SparkContext] = Some(new SparkContext("local[4]","Test"))

  override def beforeAll() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())
    super.beforeAll()
  }

  override def afterAll() {
    resetSparkContext()
  }

  override def beforeEach() {
    super.beforeEach()
  }

  override def afterEach() {
    super.afterEach()
  }


  def resetSparkContext() = {
    isc.map(LocalSparkContext.stop(_))
  }

  def getSpark(testName: String) = {
    isc match {
      case Some(tsc) => tsc
      case None =>
        val nsc = new SparkContext("local[4]", testName)
        isc = Some(nsc)
        nsc
    }
  }

  def getNewSpark(masterName: String,testName: String) = {
    isc.map(LocalSparkContext.stop(_))
    val nsc = new SparkContext(masterName,testName)
    isc = Some(nsc)
    nsc
  }

}


object LocalSparkContext extends Serializable {

  def stop(sc: SparkContext) {
    sc.stop()
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /** Runs `f` by passing in `sc` and ensures that `sc` is stopped. */
  def withSpark[T](sc: SparkContext)(f: SparkContext => T) = {
    try {
      f(sc)
    } finally {
      stop(sc)
    }
  }

  trait SilenceLogs {
    Logger.getRootLogger().removeAllAppenders()
    Logger.getRootLogger().addAppender(new NullAppender())
  }


}

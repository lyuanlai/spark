package org.apache.spark

import scala.concurrent.duration._

import org.apache.spark._
import org.apache.spark.rdd.{RDD, RDDOperationScope}

import org.apache.spark.util.{CallSite, ShutdownHookManager, Utils}

final class SplunkContext(sc_ : SparkContext, timeout_ : Duration) extends Logging {

  /**
   * Create a SplunkContext by providing the configuration necessary for a new SparkContext.
   * @param conf a org.apache.spark.SparkConf object specifying Spark parameters
   * @param timeout the time interval at which spark data will be divided into batches
   */
  def this(conf: SparkConf, timeout: Duration) = {
    this(SplunkContext.createNewSparkContext(conf), timeout)
  }

  private[spark] val sc: SparkContext = {
    if (sc_ != null) {
      sc_
    } else {
      throw new SparkException("Cannot create SplunkContext without a SparkContext")
    }
  }

  private val timeout: Duration = timeout_

  if (sc.conf.get("spark.master") == "local" || sc.conf.get("spark.master") == "local[1]") {
    logWarning("spark.master should be set as local[n], n > 1 in local mode if you have receivers" +
      " to get data, otherwise Spark jobs will not get resources to process the received data.")
  }

  private[spark] val conf = sc.conf

  private[spark] val env = sc.env

  //private val nextInputStreamId = new AtomicInteger(0)

  //private[spark] val waiter = new ContextWaiter

  //private var shutdownHookRef: AnyRef = _

  /**
   * Return the associated Spark context
   */
  def sparkContext: SparkContext = sc

  private[spark] def withScope[U](body: => U): U = sparkContext.withScope(body)

  /**
   * Execute a block of code in a scope such that all new DStreams created in this body will
   * be part of the same scope. For more detail, see the comments in `doCompute`.
   *
   * Note: Return statements are NOT allowed in the given body.
   */
  private[spark] def withNamedScope[U](name: String)(body: => U): U = {
    RDDOperationScope.withScope(sc, name, allowNesting = false, ignoreParent = false)(body)
  }
}

object SplunkContext extends Logging {
  private[spark] def createNewSparkContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }
}

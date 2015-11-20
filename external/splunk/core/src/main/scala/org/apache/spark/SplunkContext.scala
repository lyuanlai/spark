package org.apache.spark

import scala.concurrent.duration._

import org.apache.spark._
import org.apache.spark.rdd.{RDD, RDDOperationScope}

import org.apache.spark.util.{CallSite, ShutdownHookManager, Utils}

import com.splunk.{Service, ServiceArgs}

final class SplunkContext(val serviceArgs : ServiceArgs, sc_ : SparkContext) extends Logging {
  
  private[spark] val sc: SparkContext = {
    if (sc_ != null) {
      sc_
    } else {
      throw new SparkException("Cannot create SplunkContext without a SparkContext")
    }
  }

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
  //private[spark] def createNewSplunkContext(conf: SparkConf): SparkContext = {
  def createNewSplunkContext(
      username: String,
      password: String,
      host: String,
      port: Int,
      sc: SparkContext): SplunkContext = {

    val loginArgs = new ServiceArgs()
    loginArgs.setUsername(username)
    loginArgs.setPassword(password)
    loginArgs.setHost(host)
    loginArgs.setPort(port)

    new SplunkContext(loginArgs, sc)
  }

  def createNewSplunkContext(
      username: String,
      password: String,
      host: String,
      port: Int,
      conf: SparkConf): SplunkContext = {
    createNewSplunkContext(username, password, host, port, new SparkContext(conf))
  }
}

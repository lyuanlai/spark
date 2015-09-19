package org.apache.spark.streaming.dstream

import java.io.{InputStreamReader, BufferedReader, InputStream}
import java.net.Socket

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.util.WriteAheadLogUtils
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import scala.collection.JavaConversions._
import com.splunk.{Event, JobArgs, JobResultsPreviewArgs, ResultsReaderJson, Service, ServiceArgs}

object SplunkUtils {
  def createStream(
    ssc: StreamingContext,
    host: String,
    port: Int,
    username: String,
    password: String,
    search: String,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
  ): ReceiverInputDStream[Event] = {

    val serviceArgs = new ServiceArgs()
    serviceArgs.setUsername(username)
    serviceArgs.setPassword(password)
    serviceArgs.setHost(host)
    serviceArgs.setPort(port)

    val service = Service.connect(serviceArgs)
    service.login()

    //println("batchduration %s".format(ssc.graph.batchDuration))
    val batchDuration = ssc.graph.batchDuration
    val timerange = batchDuration.milliseconds / 1000
    val jobArgs = new JobArgs()
    jobArgs.setExecutionMode(JobArgs.ExecutionMode.NORMAL)
    jobArgs.setSearchMode(JobArgs.SearchMode.REALTIME)
    jobArgs.setEarliestTime("rt-%ds".format(timerange))
    jobArgs.setLatestTime("rt")
    jobArgs.setStatusBuckets(300)

    val job = service.search(search, jobArgs)
    val sid = job.getSid()

    println("utils %s %s".format(serviceArgs, sid))
    //val walEnabled = WriteAheadLogUtils.enableReceiverLog(ssc.conf)
    new SplunkInputDStream(ssc, serviceArgs, sid, storageLevel)
  }
}

private[streaming]
class SplunkInputDStream(
  @transient ssc_ : StreamingContext,
  serviceArgs: ServiceArgs,
  sid: String,
  storageLevel: StorageLevel
) extends ReceiverInputDStream[Event](ssc_) with Logging {

  def getReceiver(): SplunkReceiver = {
    println("getReceiver %s %s".format(serviceArgs, sid))
    new SplunkReceiver(serviceArgs, sid, storageLevel)
  }
}

private[streaming]
class SplunkReceiver(
  serviceArgs: ServiceArgs,
  sid: String,
  storageLevel: StorageLevel
) extends Receiver[Event](storageLevel) with Logging {

  var service: Service = null

  def onStart() {
    service = Service.connect(serviceArgs)
    service.login()

    // Start the thread that receives data over a connection
    new Thread("Splunk Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
    val job = service.getJob(sid)

    job.control("finalize")
    println("job finalized")
  }

  /** Create a splunk service connection and receive data until receiver is stopped */
  private def receive() {
    val job = service.getJob(sid)
    println("receiver %s %s".format(serviceArgs, sid))
    while (!job.isReady()) {
      println("job not ready")
      Thread.sleep(500)
    }
    try {
      val previewArgs = new JobResultsPreviewArgs()
      previewArgs.setCount(300);     // Retrieve 300 previews at a time
      previewArgs.setOutputMode(JobResultsPreviewArgs.OutputMode.JSON)
      println("previewing %s".format(job.isReady()))

      val stream : InputStream = job.getResultsPreview(previewArgs)

      val resultsReaderNormalSearch = new ResultsReaderJson(stream)
      val reader = iterableAsScalaIterable(resultsReaderNormalSearch)
      // Until stopped or connection broken continue reading
      //val reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"))
      if(!isStopped) {
        reader.foreach(event => {
          store(event)
        })
      }
      //reader.close()
      stream.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + serviceArgs.host + ":" + serviceArgs.port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}

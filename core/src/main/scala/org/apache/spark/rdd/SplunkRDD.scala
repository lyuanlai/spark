/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.language.implicitConversions

import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.concurrent.duration._
import scala.language.postfixOps
import java.util.HashMap

import org.zeromq.ZMQ
import org.zeromq.ZMQException

import org.msgpack.ScalaMessagePack
import org.msgpack.annotation.Message

import org.velvia.msgpack._
import org.velvia.msgpack.SimpleCodecs._
import org.velvia.msgpack.CollectionCodecs._
import org.velvia.MsgPack

import com.splunk.{Event, Job, JobArgs, JobResultsArgs, ResultsReaderJson, Service, ServiceArgs}

private[spark] class SplunkPartition(idx: Int, val offset: Int, val count: Int) extends Partition {
  override def index: Int = idx
}

/**
 * An RDD that executes a Splunk search command and reads results.
 * For usage example, see test case SplunkRDDSuite.
 *
 * @param service a Splunk Service object.
 * @param search the text of the query.
 * @param numPartitions the number of partitions.
 */
class SplunkRDD(
  sc: SparkContext,
  serviceArgs: ServiceArgs,
  search: String,
  numPartitions: Int)
extends RDD[Event](sc, Nil) with Logging {

  val events = scala.collection.mutable.ArrayBuffer[Event]()

  // Create a Service instance and log in with the argument map
  @transient val service = Service.connect(serviceArgs)

  // Run a normal search
  val jobargs = new JobArgs()
  jobargs.setExecutionMode(JobArgs.ExecutionMode.NORMAL)
  @transient val _job = service.getJobs().create(search, jobargs)
  val sid = _job.getSid()
  //println("init sid: %s args: %s".format(sid, serviceArgs))

  def waitForJobDone(job: Job): Unit = {
    // Wait for the search to finish
    while (!job.isDone()) {
      try {
        Thread.sleep(500)
        job.refresh()
      } catch {
        // TODO Auto-generated catch block
        case e: InterruptedException => {
          e.printStackTrace()
        }
      }
    }

  }

  override def getPartitions: Array[Partition] = {
    val service = Service.connect(serviceArgs)
    //println("getPartitions sid: %s args: %s".format(sid, serviceArgs))
    val job = service.getJob(sid)
    waitForJobDone(job)
    val length = job.getResultCount()
    val count =  Math.ceil(length.toDouble / numPartitions).toInt
    //println("length %d count %d numPartitions %d".format(length, count, numPartitions))
    (0 until numPartitions).map(i => {
      new SplunkPartition(i, i * count, count)
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Event] = {
    //context.addTaskCompletionListener{ context => closeIfNeeded() }
    var part = split.asInstanceOf[SplunkPartition]
    val service = Service.connect(serviceArgs)
    //println("compute sid: %s args: %s".format(sid, serviceArgs))
    //println("offset %d count %d".format(part.offset, part.count))
    val job = service.getJob(sid)

    waitForJobDone(job)

    // Get the search results and use the built-in XML parser to display them
    val outputargs = new JobResultsArgs()
    outputargs.setCount(part.count)
    outputargs.setOffset(part.offset)
    outputargs.setOutputMode(JobResultsArgs.OutputMode.JSON)
    val resultsNormalSearch = job.getResults(outputargs)

    try {
      val resultsReaderNormalSearch = new ResultsReaderJson(resultsNormalSearch)
      val reader = iterableAsScalaIterable(resultsReaderNormalSearch)
      reader.iterator
    } catch {
      case e: Exception => e.printStackTrace()
      List[Event]().iterator
    }
  }
}

object SplunkRDD {
  def create(
    sc: SparkContext,
    username: String,
    password: String,
    host: String,
    port: Int,
    search: String,
    numPartitions: Int): SplunkRDD = {

      // Create a map of arguments and add login parameters
      val loginArgs = new ServiceArgs()
      loginArgs.setUsername(username)
      loginArgs.setPassword(password)
      loginArgs.setHost(host)
      loginArgs.setPort(port)

      return new SplunkRDD(sc, loginArgs, search, numPartitions)
  }
} 

class SplunkCustomFunctions[T <: Map[String, Any]](rdd: RDD[T]) {
  def beamUp(connect: String): Unit = {

    val zmq = ZMQ.context(1)
    val controller = zmq.socket(ZMQ.PUSH)
    @transient val sc = rdd.context
    val numpart = rdd.partitions.size
    controller.connect(connect)
    controller.send(ScalaMessagePack.write(("partitions", numpart)))

    rdd.foreachPartition(iter => {
      val zmq = ZMQ.context(1)
      val sender = zmq.socket(ZMQ.PUSH)
      sender.connect(connect)

      iter.foreach(x => {
        val payload = ScalaMessagePack.write(x)
        sender.send(payload, 0)
      })

      sender.send(ScalaMessagePack.write("complete"))
      sender.close()
    })
  }
}

object SplunkCustomFunctions {
  implicit def addSplunkCustomFunctions[T <: Map[String, Any]](rdd: RDD[T]) = new SplunkCustomFunctions[T](rdd) 
}

class SplunkPipeRDD(
  @transient sc: SparkContext,
  bind: String,
  connect: String,
  numPartitions: Int
  )
extends RDD[Map[String, String]](sc, Nil) {

  type PipeEvent = Map[String, String]

  override def getPartitions: Array[Partition] = {
    val zmq = ZMQ.context(1)
    val receiver = zmq.socket(ZMQ.REQ)
    receiver.connect(connect)

    receiver.send("size".getBytes(), 0)
    val m = receiver.recv(0)
    val length = ScalaMessagePack.read[Int](m)
    val count =  Math.ceil(length.toDouble / numPartitions).toInt
    println("get length %d".format(length))

    receiver.close()
    println("length %d count %d numPartitions %d".format(length, count, numPartitions))
    (0 until numPartitions).map(i => new SplunkPartition(i, i*count, count)).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[PipeEvent] = {
    var part = split.asInstanceOf[SplunkPartition]
    var i = part.offset
    val end = part.offset + part.count
    var wait = 0

    val zmq = ZMQ.context(1)
    val receiver = zmq.socket(ZMQ.REQ)
    val events = scala.collection.mutable.ArrayBuffer[PipeEvent]()

    println("compute %d %d %d %s".format(part.index, part.offset, part.count, connect))
    receiver.connect(connect)

    while (i < end && wait < 30) {
      try {
        receiver.send("data".getBytes(), ZMQ.SNDMORE)
        receiver.send(ScalaMessagePack.write(i), 0)
        var m: Array[Byte] = null

        do {
          m = receiver.recv(ZMQ.DONTWAIT)
          m match {
            case m: Array[Byte] => 
              val unpacked = ScalaMessagePack.read[PipeEvent](m)
              println("message %d".format(i))
              //println("%s".format(unpacked._2))
              events += unpacked
              i += 1
            case null =>
              Thread.sleep(500)
              wait += 1
          }
        } while (m == null && wait < 30)
      } catch {
        case e: org.zeromq.ZMQException =>
          println("sleep")
          Thread.sleep(500)
          wait += 1
        case x: Throwable =>
          println(x)
      }
    }
    
    receiver.close()
    events.iterator()
  }

  //override def collect[U](f: PartialFunction[T, U])(implicit arg0: ClassTag[U]): RDD[U] = {
  //}

}

class MySparkListener extends SparkListener {
  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("applicationEnd")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("jobEnd")
  }
}

/**
 * This is a helper class that wraps the SplunkUtils.createStream() into more
 * Python-friendly class and function so that it can be easily
 * instantiated and called from Python's SplunkUtils (see SPARK-6027).
 *
 * The zero-arg constructor helps instantiate this class from the Class object
 * classOf[SplunkUtilsPythonHelper].newInstance(), and the createStream()
 * takes care of known parameters instead of passing them from Python
 */
private class SplunkUtilsPythonHelper {
  /* def createStream(
    ssc: StreamingContext,
    storageLevel: StorageLevel): ReceiverInputDStream[Event] = {
      SplunkUtils.createStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
        jssc,
        classOf[Array[Byte]],
        classOf[Array[Byte]],
        classOf[DefaultDecoder],
        classOf[DefaultDecoder],
        kafkaParams,
        topics,
        storageLevel)
    }*/

   def createRDD(
     sc: SparkContext,
     username: String,
     password: String,
     host: String,
     port: Int,
     search: String,
     numPartitions: Int): RDD[Event] = {
       SplunkRDD.create(sc, username, password, host, port, search, numPartitions)
   }

   def createPipeRDD(sc: SparkContext): Unit = {
   }
}

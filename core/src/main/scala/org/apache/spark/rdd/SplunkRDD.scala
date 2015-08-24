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
import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.concurrent.duration._
import scala.language.postfixOps
import akka.zeromq._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorLogging
import akka.serialization.SerializationExtension
import akka.util.ByteString
import java.lang.management.ManagementFactory

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
 *   The query must contain two ? placeholders for parameters used to partition the results.
 *   E.g. "select title, author from books where ? <= id and id <= ?"
 * @param numPartitions the number of partitions.
 *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
 *   the query would be executed twice, once with (1, 10) and once with (11, 20)
 */
class SplunkRDD(
  sc: SparkContext,
  serviceArgs: ServiceArgs,
  search: String,
  numPartitions: Int)
extends RDD[Event](sc, Nil) with Logging {

  val events = scala.collection.mutable.ListBuffer[Event]()

  // Create a Service instance and log in with the argument map
  @transient val service = Service.connect(serviceArgs)

  // Run a normal search
  val jobargs = new JobArgs()
  jobargs.setExecutionMode(JobArgs.ExecutionMode.NORMAL)
  @transient val _job = service.getJobs().create(search, jobargs)
  val sid = _job.getSid()
  println("init sid: %s args: %s".format(sid, serviceArgs))

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
    println("getPartitions sid: %s args: %s".format(sid, serviceArgs))
    val job = service.getJob(sid)
    waitForJobDone(job)
    val length = job.getResultCount()
    val count =  Math.ceil(length.toDouble / numPartitions).toInt
    println("length %d count %d numPartitions %d".format(length, count, numPartitions))
    (0 until numPartitions).map(i => {
      new SplunkPartition(i, i * count, count)
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Event] = {
    //context.addTaskCompletionListener{ context => closeIfNeeded() }
    var part = split.asInstanceOf[SplunkPartition]
    val service = Service.connect(serviceArgs)
    println("compute sid: %s args: %s".format(sid, serviceArgs))
    println("offset %d count %d".format(part.offset, part.count))
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

class SplunkPipeRDD(
  sc: SparkContext,
  bind: String,
  numPartitions: Int)
extends RDD[Event](sc, Nil) with Logging {

  implicit val system = ActorSystem("demo")
  
  case object Tick
  case class Heap(timestamp: Long, used: Long, max: Long)
  case class Load(timestamp: Long, loadAverage: Double)
  
  class SplunkPipe extends Actor with ActorLogging {
  
    val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep, Listener(self), Bind("tcp://*:5555"))
    //val pubSocket = ZeroMQExtension(context.system).newSocket(SocketType.Pub,
    //  Bind("tcp://127.0.0.1:1235"))
    val memory = ManagementFactory.getMemoryMXBean
    val os = ManagementFactory.getOperatingSystemMXBean
    val ser = SerializationExtension(context.system)
    import context.dispatcher
  
    override def preStart() {
      context.system.scheduler.schedule(1 second, 1 second, self, Tick)
    }
  
    override def postRestart(reason: Throwable) {
      // don't call preStart, only schedule once
    }
  
    def receive: Receive = {
      case Connecting => println("Connecting")
      case m: ZMQMessage => println("got message " + m.frame(0)); repSocket ! m
      case Tick =>
        val currentHeap = memory.getHeapMemoryUsage
        val timestamp = System.currentTimeMillis
  
        // use akka SerializationExtension to convert to bytes
        val heapPayload = ser.serialize(Heap(timestamp, currentHeap.getUsed,
          currentHeap.getMax)).get
        // the first frame is the topic, second is the message
        repSocket ! ZMQMessage(ByteString("health.heap"), ByteString(heapPayload))
  
        // use akka SerializationExtension to convert to bytes
        val loadPayload = ser.serialize(Load(timestamp, os.getSystemLoadAverage)).get
        // the first frame is the topic, second is the message
        repSocket ! ZMQMessage(ByteString("health.load"), ByteString(loadPayload))
    }
  }
  
  val server = system.actorOf(Props[SplunkPipe], "splunkpipe")

  val events = scala.collection.mutable.ListBuffer[Event]()

  override def getPartitions: Array[Partition] = {
    val length = events.size
    val count =  Math.ceil(length.toDouble / numPartitions).toInt
    println("length %d count %d numPartitions %d".format(length, count, numPartitions))
    (0 until numPartitions).map(i => {
      new SplunkPartition(i, i * count, count)
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Event] = {
    events.iterator()
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

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
import akka.zeromq._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorLogging
import akka.pattern.ask
import akka.serialization.SerializationExtension
import akka.util.ByteString
import akka.util.Timeout
import java.util.HashMap

import org.zeromq.ZMQ

import org.msgpack.ScalaMessagePack;
import org.msgpack.annotation.Message;

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

class SplunkCustomFunctions[K, V](rdd: RDD[(K, V)]) {
  def beamUp(connect: String): Unit = {

    rdd.foreachPartition(iter => {
      val zmq = ZMQ.context(1)
      val sender = zmq.socket(ZMQ.PUSH)
      sender.connect(connect)

      iter.foreach(x => {
        val payload = ScalaMessagePack.write(x)
        sender.send(payload, payload.size)
      })

      sender.close()
    })
  }
}

object SplunkCustomFunctions {
  implicit def addSplunkCustomFunctions[K, V](rdd: RDD[(K, V)]) = new SplunkCustomFunctions[K, V](rdd) 
}

class SplunkPipeRDD(
  @transient sc: SparkContext,
  bind: String,
  connect: String,
  numPartitions: Int,
  implicit val timeout: Timeout = Timeout(30 seconds)
  )
extends RDD[Map[String, String]](sc, Nil) {

  type PipeEvent = Map[String, String]

  case object Collect
  case class Beam(data: List[PipeEvent])
  var complete = false
  val events = scala.collection.mutable.ArrayBuffer[PipeEvent]()
  
  class Channel extends Actor with ActorLogging {
    val controller = ZeroMQExtension(context.system).newSocket(SocketType.Sub, Listener(self), Connect(connect), Subscribe(""))

    def receive: Receive = {
      case Connecting => println("Channel Connecting %s".format(this.self.path))
      case m: ZMQMessage if m.frames(0).utf8String == "Stop" =>
        println("shutting down")
        context.system.shutdown()
    }
  }

  class SplunkPipe extends Actor with ActorLogging {
  
    //val repSocket = ZeroMQExtension(context.system).newSocket(SocketType.Rep, Listener(self), Bind("tcp://*:5555"))
    //val pubSocket = ZeroMQExtension(context.system).newSocket(SocketType.Pub,
    //  Bind("tcp://127.0.0.1:1235"))
    val ser = SerializationExtension(context.system)

    val zmq = ZeroMQExtension(context.system)
    val receiver = zmq.newSocket(SocketType.Req, Listener(self), Connect("tcp://minion1:5557"), ReconnectIVL(60))
    val events = scala.collection.mutable.ArrayBuffer[PipeEvent]()

    //val items = context.poller
    //items.register(receiver, ZMQ.Poller.POLLIN)
    //items.register(controller, ZMQ.Poller.POLLIN)

    import context._
  
    override def preStart() {
      //context.system.scheduler.schedule(1 second, 1 second, self, "ready2")(context.dispatcher, receiver)
    }
  
    override def postRestart(reason: Throwable) {
      // don't call preStart, only schedule once
    }

    def receive: Receive = {
      case Connecting => println("Connecting %s".format(this.self.path))
        receiver ! ZMQMessage(ByteString("ready"))
      case m: ZMQMessage if m.frames(0).utf8String == "event" =>
        println("event %s".format(this.self.path))
        val deserialized: Map[String, String] = ScalaMessagePack.read[Map[String, String]](m.frames(1).toArray)
        events += deserialized
  
        // use akka SerializationExtension to convert to bytes
        //val heapPayload = ser.serialize(Heap(timestamp, currentHeap.getUsed,
        //  currentHeap.getMax)).get
        // the first frame is the topic, second is the message
        //pusher ! ZMQMessage(ByteString("health.heap"), ByteString(heapPayload))
        receiver ! ZMQMessage(ByteString("ready"))

      case m: ZMQMessage if m.frames(0).utf8String == "Complete" =>
        complete = true
        println("complete, ")
      case m: ZMQMessage =>
        println("%s".format(m.frames(0).utf8String))
      case "pull" =>
        receiver ! ZMQMessage(ByteString("ready"))
      case Collect =>
        println("Collect")
        if (complete) {
          println("send %s %d".format(this.self.path, events.size))
          sender ! events
        } else {
          sender ! "noop " + this.self.path
        }
    }

    def ready: Unit = {
      receiver ! ZMQMessage(ByteString("ready"))
    }
  }
  
  /*sys addShutdownHook {
    system.shutdown()
    println("actorsystem shutdown")
  }*/

  override def getPartitions: Array[Partition] = {
    val length = events.size
    val count =  Math.ceil(length.toDouble / numPartitions).toInt
    println("length %d count %d numPartitions %d".format(length, count, numPartitions))
    (0 until numPartitions).map(i => {
      new SplunkPartition(i, i * count, count)
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Map[String, String]] = {
    println("computing %d".format(split.index))
    val system = ActorSystem("demo")
    val piper = system.actorOf(Props(classOf[SplunkPipe], this), "splunkpipe" + split.index.toString)
    var i = 0
    var done = false

    println("actor path [%s]".format(piper.path))
  
    do {
      val future = ask(piper, Collect)
      val results = Await.result(future, timeout.duration)
      
      results match {
        case results: scala.collection.mutable.ArrayBuffer[PipeEvent] => 
          println("RESULTS-%d %d %s".format(split.index, results.size, results.getClass.getName))
          events ++= results
          done = true
        case _ => 
          println("%d %s".format(split.index, results))
      }
      i += 1
      println("compute-%d %d".format(split.index, i))
      if (!done) {
        Thread.sleep(500)
      }
    } while (!done)
    
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

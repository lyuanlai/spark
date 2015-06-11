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

import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import com.splunk.{Application, Event, JobExportArgs, ResultsReaderJson, Service, ServiceArgs, SearchResults}

private[spark] class SplunkPartition(idx: Int) extends Partition {
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
    service: Service,
    search: String,
    numPartitions: Int)
  extends RDD[Event](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    // bounds are inclusive, hence the + 1 here and - 1 on end
    (0 until numPartitions).map(i => {
      new SplunkPartition(i)
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Event] = /*new NextIterator[Event]*/ {
    context.addTaskCompletionListener{ context => closeIfNeeded() }
    val part = split.asInstanceOf[SplunkPartition]

    // Create an argument map for the export arguments
    val exportArgs = new JobExportArgs()
    exportArgs.setEarliestTime("-1h")
    exportArgs.setLatestTime("now")
    exportArgs.setSearchMode(JobExportArgs.SearchMode.NORMAL)
    exportArgs.setOutputMode(JobExportArgs.OutputMode.JSON)

    // Run the search with a search query and export arguments
    val mySearch = "search index=_internal"
    val exportSearch = service.export(mySearch, exportArgs)

    // Display results using the SDK's multi-results reader for XML 
    //val multiResultsReader = MultiResultsReaderJson(exportSearch)

    //multiResultsReader.foreach(searchResults => {
    //  searchResults.foreach(event => {
    //    event.keySet().foreach(key => {
    //      System.out.println("   " + key + ":  " + event.get(key))
    //    })
    //  })
    //})

    new ResultsReaderJson(exportSearch)

    /*override def getNext(): Event = {
      //try {
        searchResults.getNextEvent()
      //} catch {
      //  null.asInstanceOf[Event]
      //}
    }*/

    //override def close() {
    //  multiResultsReader.close()
    //}
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

    // Create a Service instance and log in with the argument map
    val service = Service.connect(loginArgs)

    // A second way to create a new Service object and log in
    // service = new Service("localhost", 8089)
    // service.login("admin", "changeme")

    // A third way to create a new Service object and log in
    // Service service = new Service(loginArgs)
    // service.login()

    return new SplunkRDD(sc, service, search, numPartitions)
  }
} 

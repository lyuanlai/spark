#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from py4j.java_gateway import Py4JJavaError

from pyspark.rdd import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.streaming import DStream
from pyspark.streaming.dstream import TransformedDStream
from pyspark.streaming.util import TransformFunction

__all__ = ['SplunkUtils']


class SplunkUtils(object):

    #@staticmethod
    #def createStream(ssc, zkQuorum, groupId, topics, kafkaParams={},
    #                 storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
    #                 keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
    #    """
    #    Create an input stream that pulls messages from a Kafka Broker.

    #    :param ssc:  StreamingContext object
    #    :param zkQuorum:  Zookeeper quorum (hostname:port,hostname:port,..).
    #    :param groupId:  The group id for this consumer.
    #    :param topics:  Dict of (topic_name -> numPartitions) to consume.
    #                    Each partition is consumed in its own thread.
    #    :param kafkaParams: Additional params for Kafka
    #    :param storageLevel:  RDD storage level.
    #    :param keyDecoder:  A function used to decode key (default is utf8_decoder)
    #    :param valueDecoder:  A function used to decode value (default is utf8_decoder)
    #    :return: A DStream object
    #    """
    #    kafkaParams.update({
    #        "zookeeper.connect": zkQuorum,
    #        "group.id": groupId,
    #        "zookeeper.connection.timeout.ms": "10000",
    #    })
    #    if not isinstance(topics, dict):
    #        raise TypeError("topics should be dict")
    #    jlevel = ssc._sc._getJavaStorageLevel(storageLevel)

    #    try:
    #        # Use SplunkUtilsPythonHelper to access Scala's SplunkUtils (see SPARK-6027)
    #        helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader()\
    #            .loadClass("org.apache.spark.streaming.kafka.SplunkUtilsPythonHelper")
    #        helper = helperClass.newInstance()
    #        jstream = helper.createStream(ssc._jssc, kafkaParams, topics, jlevel)
    #    except Py4JJavaError as e:
    #        # TODO: use --jar once it also work on driver
    #        if 'ClassNotFoundException' in str(e.java_exception):
    #            SplunkUtils._printErrorMsg(ssc.sparkContext)
    #        raise e
    #    ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
    #    stream = DStream(jstream, ssc, ser)
    #    return stream.map(lambda k_v: (keyDecoder(k_v[0]), valueDecoder(k_v[1])))

    @staticmethod
    def createRDD(sc, host, port, username, password, search, numPartitions=4):
        """
        .. note:: Experimental

        Create a RDD from Kafka using offset ranges for each topic and partition.

        :param sc:  SparkContext object
        :param host: splunkd host
        :param port: splunkd port
        :param username : username
        :param password: password
        :param search: search string
        :param numPartitions: number of partitions
        :return: A RDD object
        """
        #if not isinstance(kafkaParams, dict):
        #    raise TypeError("kafkaParams should be dict")
        #if not isinstance(offsetRanges, list):
        #    raise TypeError("offsetRanges should be list")

        try:
            helperClass = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.rdd.SplunkUtilsPythonHelper")
            helper = helperClass.newInstance()
            jrdd = helper.createRDD(sc._jsc, host, port, username, password,
                search, numPartitions)
        except Py4JJavaError as e:
            if 'ClassNotFoundException' in str(e.java_exception):
                SplunkUtils._printErrorMsg(sc)
            raise e

        return jrdd

    @staticmethod
    def _printErrorMsg(sc):
        print("""
________________________________________________________________________________________________

  Spark Streaming's Splunk libraries not found in class path. Try one of the following.

  1. Include the Kafka library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-splunk:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-splunk-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-splunk-assembly.jar> ...

________________________________________________________________________________________________

""" % (sc.version, sc.version))

class SplunkRDD(RDD):
    """
    A Python wrapper of SplunkRDD, to provide additional information on normal RDD.
    """

    def __init__(self, jrdd, ctx, jrdd_deserializer):
        RDD.__init__(self, jrdd, ctx, jrdd_deserializer)

class SplunkDStream(DStream):
    """
    A Python wrapper of SplunkDStream
    """

    def __init__(self, jdstream, ssc, jrdd_deserializer):
        DStream.__init__(self, jdstream, ssc, jrdd_deserializer)

    def foreachRDD(self, func):
        """
        Apply a function to each RDD in this DStream.
        """
        if func.__code__.co_argcount == 1:
            old_func = func
            func = lambda r, rdd: old_func(rdd)
        jfunc = TransformFunction(self._sc, func, self._jrdd_deserializer) \
            .rdd_wrapper(lambda jrdd, ctx, ser: SplunkRDD(jrdd, ctx, ser))
        api = self._ssc._jvm.PythonDStream
        api.callForeachRDD(self._jdstream, jfunc)

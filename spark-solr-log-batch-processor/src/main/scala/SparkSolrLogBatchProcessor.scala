import org.apache.solr.common.SolrInputDocument
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.OffsetRange

import kafka.serializer.StringDecoder
import java.util.Date

@SerialVersionUID(100L)
class SolrQueryLog(var timestamp: Long, var level: String, var collection: String,
    var shard: Int, var replica: Int, var params: String, var hits: Int, var status: Int,
    var qtime: Int, val isFacet: Boolean)
extends Serializable {
  
}

class OpenTSDBMetric(var timestamp: Long, var name: String, var value: Float, var tags: String) {

}

object SparkSolrLogBatchProcessor { 
  def printHelp() {
    println("SparkSolrLogBatchProcessor l(ocal)/c(luster) zk_host kafka_broker_list kafka_topic kafka_offset_start " + 
        "kakfa_offset_end log_start_time log_end_time resample_bucket_size solr_collection batch_size_for_solr_indexing " +
        "slow_query_min_qtime opentsdb_host")
  }
  
  def main(args: Array[String]) {
    
    if (args.size < 13) {
      printHelp()
      return      
    }
    
    val localMode = if (args(0) == "l") true else false
    val zk_host = args(1)
    
    val kafkaBrokerList = args(2) 
    val kafkaTopic = args(3) 
    val kafkaOffsetStart = args(4).toLong
    val kafkaOffsetEnd = args(5).toLong
    
    val logStartTime = args(6).toLong
    val logEndTime = args(7).toLong
    
    val resampleBucket = args(8).toInt

    val solrCollectionName = args(9)
    val solrIndexBatchSize = args(10).toInt
    val slowQueryMinQTime = args(11).toInt

    val openTsdbURL = "http://" + args(12) + ":4242/api/put"
        
    val tags = "\"source\": \"solr\"" 
    
    println("Exec mode:" + (if (localMode) "local" else "cluster"))
    println("Zookeeper host:" + zk_host)
    println("kafka broker list:" + kafkaBrokerList)
    println("kafka topic:" + kafkaTopic)
    println("Kafka offset range:" + kafkaOffsetStart + " to " + kafkaOffsetEnd)
    println("Log time range:" + logStartTime + " to " + logEndTime)
    println("Resampling bucket size:" + resampleBucket)
    println("Solr collection name:" + solrCollectionName)
    println("Batch size for Solr indexing:" + solrIndexBatchSize)    
    println("Min qtime for slow queries:" + slowQueryMinQTime)
    println("OpenTSDB URL:" + openTsdbURL)
    
    val sc =  if (localMode) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val conf = new SparkConf().setAppName("Simple Solr Log Batch Processor")
      new SparkContext(conf) 
    }
  
   // pull logs from Kafka to RDD
    
    val offsetRanges = Array(
      OffsetRange(kafkaTopic, 0, kafkaOffsetStart, kafkaOffsetEnd)
    )

    val kafkaParams = Map("metadata.broker.list" -> kafkaBrokerList)
    
    def gtok(tuple: (String, String)): (Long, SolrQueryLog) = { 
      val log = tuple._2
      if (log.size < 23) return null
      
      val timestamp = log.substring(0, 23).replace(",", ".")

    /* matches 
     * 0 entire string
     * 1 timestamp
     * 2 collection
     * 3 shard
     * 4 replica
     * 5 params
     * 6 hits
     * 7 status
     * 8 qtime
     */
    
     val pSolrLogPrefix = "(20\\d\\d-\\d\\d-\\d\\d\\s+\\d\\d:\\d\\d:\\d\\d,\\d\\d\\d)\\s+INFO org\\.apache\\.solr\\.core\\.SolrCore\\.Request: \\[(.+)_shard(\\d+)_replica(\\d+)\\] "
     val pQuery = (pSolrLogPrefix + "webapp=\\/solr path=\\/select params=\\{(.+)\\} hits=(\\d+) status=(\\d+) QTime=(\\d+).*").r
     val pIsShard = ".+isShard=(.+?)[&|\\}].+".r
    
     val mQuery = pQuery.findAllMatchIn(log)
     if (mQuery.hasNext) {
       val m = mQuery.next()
       val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")
       try {
         val t = format.parse(m.group(1)).getTime()/1000  // use seconds
         if (t < logStartTime || t >= logEndTime) null
         val mIsShard = pIsShard.findAllMatchIn(m.group(5))
         if (mIsShard.hasNext && mIsShard.next().toString() == "true")  null
         else (t, new SolrQueryLog(t, "", m.group(2), m.group(3).toInt, m.group(4).toInt,
               m.group(5), m.group(6).toInt, m.group(7).toInt, m.group(8).toInt, 
               m.group(5).contains("facet"))) 
       }
       catch {
         case _:Exception => null
       }
     } else {
       null
     }
    }
     
    val allLogsRdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, offsetRanges)
      
    val queryLogsRDD = allLogsRdd.map(gtok).filter(_ != null)
    
    val queryLogsCount = queryLogsRDD.count()
    val queryQTimeSum = queryLogsRDD.map { case (timestamp, log) => log.qtime }
                                    .reduce((qtime1, qtime2) => (qtime1 + qtime2))
    
    val queryQtimeAvg = queryQTimeSum / queryLogsCount
    val queryQPS = queryLogsCount / (logEndTime - logStartTime)
    println("Solr QPS " + queryQPS.toString() + " latency " + queryQtimeAvg.toString())

    val metricNameQPSOverall = "query.qps.overall"
    val metricNameLatencyOverall = "query.latency.overall"
    OpenTsdbSupport.postMetric(openTsdbURL, new OpenTSDBMetric(logStartTime, metricNameQPSOverall, queryQPS, tags))
    OpenTsdbSupport.postMetric(openTsdbURL, new OpenTSDBMetric(logStartTime, metricNameLatencyOverall, queryQtimeAvg, tags))

    val facetQueryLogs = queryLogsRDD.filter { case (timestamp, log) => log.isFacet }
    val facetQueryLogsCount = facetQueryLogs.count()
    if (facetQueryLogsCount != 0) {
      val facetQueryQTimeSum = facetQueryLogs.map { case (timestamp, log) => log.qtime }
                                             .reduce((qtime1, qtime2) => (qtime1 + qtime2))
      val facetQtimeAvg = facetQueryQTimeSum / facetQueryLogsCount
      val facetQPS = facetQueryLogsCount / (logEndTime - logStartTime)
      val metricNameQPSFacet = "query.qps.facet"
      val metricNameLatencyFacet = "query.latency.facet"
      OpenTsdbSupport.postMetric(openTsdbURL, new OpenTSDBMetric(logStartTime, metricNameQPSFacet, facetQPS, tags))
      OpenTsdbSupport.postMetric(openTsdbURL, new OpenTSDBMetric(logStartTime, metricNameLatencyFacet, facetQtimeAvg, tags))
    }

    val metricNameQPS = "query.qps"
    val metricNameLatency = "query.latency"
                              
    val queryTimeReSampledRDD = queryLogsRDD.map { case (timestamp, log) => ((timestamp / resampleBucket) * resampleBucket , (log.qtime, 1L)) }
                              .reduceByKey { case ((qtime1, count1), (qtime2, count2)) => (qtime1 + qtime2, count1 + count2) }
                              .map { case (timestamp, (qtime, count)) => (timestamp, (qtime/count, count)) }
                              
    val queryResampledQPSRDD = queryTimeReSampledRDD.map {
       case (timestamp, (qtime_avg, count)) => new OpenTSDBMetric(timestamp, metricNameQPS, count/resampleBucket, tags)
    }
    val queryResampledLatencyRDD = queryTimeReSampledRDD.map {
       case (timestamp, (qtime_avg, count)) => new OpenTSDBMetric(timestamp, metricNameLatency, qtime_avg, tags)
    }
    OpenTsdbSupport.postMetrics(openTsdbURL, queryResampledQPSRDD)
    OpenTsdbSupport.postMetrics(openTsdbURL, queryResampledLatencyRDD)
    
    val slowQueryRDD = queryLogsRDD.filter{
      case (timestamp, log) => log.qtime >= slowQueryMinQTime
    }
    
    def solrQueryLog2SolrDoc(q: SolrQueryLog) : SolrInputDocument = {
        val doc = new SolrInputDocument()
        
        doc.addField("timestamp", new Date(q.timestamp * 1000))
        doc.addField("source", "solr")
        doc.addField("collection", q.collection)
        doc.addField("shard", q.shard)
        doc.addField("replica", q.replica)
        doc.addField("params", q.params)
        doc.addField("hits", q.hits)
        doc.addField("status", q.status)
        doc.addField("qtime", q.qtime)
      
        doc
    }
    
    val slowQuerySolrRDD = slowQueryRDD.values.map(solrQueryLog2SolrDoc)
    SolRSupport.indexDoc(zk_host, solrCollectionName, solrIndexBatchSize, slowQuerySolrRDD)        
  }
}



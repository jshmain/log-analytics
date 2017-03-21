import java.net.{ConnectException, SocketException}
import java.util

import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.spark.rdd.RDD
import org.apache.solr.client.solrj.impl.CloudSolrServer
import scala.collection.JavaConverters._

object CloudSolRServerBuilder {
  val obj = new Object
  var cachedSolRServer: CloudSolrServer = null

  def build(zkHost: String): CloudSolrServer = {
    if (cachedSolRServer != null) {
      cachedSolRServer
    } else {
      obj.synchronized {
        if (cachedSolRServer == null) {
          cachedSolRServer = new CloudSolrServer(zkHost)
        }
      }
      cachedSolRServer
    }
  }
}

object SolRSupport {

  def indexDoc(zkHost: String,
               collection: String,
               batchSize: Int,
               docRdd: RDD[SolrInputDocument]): Unit = 
    docRdd.foreachPartition{ it => 
      val solrServer = CloudSolRServerBuilder.build(zkHost)
      it.grouped(batchSize).foreach{ batch => sendBatchToSolr(solrServer, collection, batch.asJava) }
    } 

  def sendBatchToSolr(solrServer: CloudSolrServer,
                      collection: String,
                      batch: util.Collection[SolrInputDocument]) {
    val req = new UpdateRequest()
    req.setParam("collection", collection)
    
    req.add(batch)
    try {
      solrServer.request(req)
    } catch  {
      case e:Exception => {
        if (shouldRetry(e)) {
          try {
            Thread.sleep(2000)
          } catch {
            case e1: InterruptedException => {
              Thread.interrupted()
            }
          }

          try {
            solrServer.request(req)
          } catch {
            case e1: Exception => {

              if (e1.isInstanceOf[RuntimeException]) {
                throw e1.asInstanceOf[RuntimeException]
              } else {
                throw new RuntimeException(e1)
              }
            }
          }
        } else {
          if (e.isInstanceOf[RuntimeException]) {
            throw e.asInstanceOf[RuntimeException]
          } else {
            throw new RuntimeException(e)
          }
        }
      }
    } finally {
      batch.clear()
    }
  }

  def shouldRetry(exc: Exception): Boolean = {
    val rootCause = SolrException.getRootCause(exc)
    rootCause.isInstanceOf[ConnectException] ||
      rootCause.isInstanceOf[SocketException]
  }
}

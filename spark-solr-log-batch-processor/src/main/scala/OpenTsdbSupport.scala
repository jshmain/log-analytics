import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.spark.rdd.RDD


object OpenTsdbSupport {

  def postMetrics(url: String,
                 metricRdd: RDD[OpenTSDBMetric]) = 
    metricRdd.foreachPartition {
      _.foreach { metric => postMetric(url, metric) }
    }

  def postMetric(url: String,
                 metric: OpenTSDBMetric) = {

    val client = new DefaultHttpClient
    val post = new HttpPost(url)
    
		val input = new StringEntity("{" + 
		    "\"metric\": \"" + metric.name + "\"," +
		    "\"timestamp\": " + metric.timestamp + "," +
		    "\"value\": " + metric.value + "," +
		    "\"tags\": { " + metric.tags + "} }");
		input.setContentType("application/json");
		post.setEntity(input);

    val response = client.execute(post)  
    
    //response.getStatusLine().getStatusCode()
  }
}

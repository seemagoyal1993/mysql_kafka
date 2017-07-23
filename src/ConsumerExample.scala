import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._
import java.io._
  import java.util.Properties


object ConsumerExample extends App {
  val TOPIC="test"
  val  props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")
    props.put("autooffset.reset", "smallest");  

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while(true){
    val records=consumer.poll(100)
    for (record<-records.asScala){
      val pw = new PrintWriter(new File("file:///home/seema/Desktop/consumes.txt" ))
      pw.write(record.toString())
     pw.close()
     
    }
   
       
 
}

}
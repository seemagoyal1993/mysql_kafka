import java.util.Properties
import org.apache.kafka.clients.producer._
import java.sql.ResultSet


/**
 * @author Seema.Goyal
 */
object ProducerExample {
  def main(args: Array[String])
  { val dbconnect=new DbConnection
     val ro = dbconnect.dbdata()
     val props=new Properties();
 props.put("bootstrap.servers","localhost:9092") //Connecting to Topic
  /*Configuration to Producer*/
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
 props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
  val producer=new KafkaProducer[String,String](props)
   val TOPIC="test" 
 
     println("db connected")
     
     
    
//     println(ro.toList)
  
 var r= for (line <- ro ) yield{(line.toList.map(x=>x._2).mkString(","))}
 
 for (line <- r) {println(line)
 
  var record = new ProducerRecord(TOPIC,"key", s"hello$line ")
   producer.send(record)

 
 
 }
  
 producer.close()

  
  }
}
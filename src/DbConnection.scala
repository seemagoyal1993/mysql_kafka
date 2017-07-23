import java.sql.DriverManager
import java.sql.Connection

/**
 * @author Seema.Goyal
 */
class DbConnection {
var rows:Array[Map[String,String]] = Array()
    val user="root"
    Class.forName("com.mysql.jdbc.Driver")
    val pass="redhat"
    val url="jdbc:mysql://127.0.0.1:3306/"
    val connection:Connection=DriverManager.getConnection(url,user,pass)
    val statement = connection.createStatement()
    val rs= statement.executeQuery("SELECT userid,email FROM users.users")
    val columns="userid,email"

    def dbdata() :Array[Map[String,String]] = {
  
while(rs.next){
 
  var row: Array[String] = Array() 
  for (col <- columns.split(","))
  {
    row = row :+ rs.getString(col).toString()
    }   
  val rowMap = columns.split(",").zip(row).toMap

 rows = rows :+ rowMap

  }
rows
 
      }
    
  
 
}
 
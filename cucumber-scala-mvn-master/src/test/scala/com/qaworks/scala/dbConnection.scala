package com.qaworks.scala
import java.util.Properties
object dbConnection {
  
  
  
  
  val url = "jdbc:db2://cap-sg-prd-4.securegateway.appdomain.cloud:19881/LP01CDA1"


 val properties = new Properties()
 properties.put("user","i0008ei" )
 properties.put("password", "goutham2")
 Class.forName("com.ibm.db2.jcc.DB2Driver")

  
  
  
}
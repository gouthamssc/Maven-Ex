package com.qaworks.scala

import cucumber.api.scala.{EN, ScalaDsl}
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SparkSession
import java.util.Properties


class date_tab_def extends ScalaDsl with EN {
  
  val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  
     val url = "jdbc:db2://cap-sg-prd-4.securegateway.appdomain.cloud:19881/LP01CDA1"


 val properties = new Properties()
 properties.put("user","i0008ei" )
 properties.put("password", "goutham2")
 Class.forName("com.ibm.db2.jcc.DB2Driver")
 val CONFIG_CYCLE = "DBBFMSXR.CONFIG_CYCLE"

   
  Given("""^user creditials$"""){ () =>
  
   
    println("given cedientials")
    
}
  
  
  
When("""^user validtaes db cnctn$"""){ () =>
 

  
spark.read.jdbc(url, CONFIG_CYCLE, properties).select("EFFC_DATE", "DLET_DATE","CYCLE_ID").show()
  
}



  Then("""^success$"""){ () =>
  
    println("success")
    
}
  
  
}
package Functions

import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

class DatabaseConnection {
  
  def getConnectionProperties(): Properties ={
    val devProperties = new Properties()
    devProperties.put("user", getDBUserName())
    devProperties.put("password", getDBPassword())
    devProperties.put("sslConnection", "true")
    devProperties.put("sslTrustStoreLocation", getDBJKSFile())
    devProperties
  }

  def getDBUserName(): String = {
    val username = "i0008ei"
    username
  }

  
  def getDBPassword(): String = {
    val password = "goutham2"
    password
  }

  def getDBJKSFile(): String = {

    val jksfilelocation = "/Users/gouthamrajanala/Downloads/ibm-gold-truststore.jks"
    jksfilelocation
  }

  def getDBport(): String = {
    val port = "38362"
    port
  }

  def getDBip(): String = {
    val ip = "9.212.130.213"
    ip
  }

  def getDBName(): String = {
    val name = "LP01CDA1"
    name
  }

  def getDBUrl(): String = {
    val url = "jdbc:db2://" + getDBip() + ":" + getDBport() + "/" +getDBName()
    url
//val url =   "jdbc:db2://cap-sg-prd-4.securegateway.appdomain.cloud:19881/LP01CDA1"
// url
  }

  def getDBSchema(): String = {
    val schema = "DBBFMSXR"
    schema
  }

  
}

package com.qaworks.scala

import org.apache.spark.sql.SparkSession
import java.util.ResourceBundle
import org.apache.spark.sql.DataFrame
import java.util.Properties

object DATE_TAB {
  
  def  main(args: Array[String]): Unit = {
  
  read()


  
 

  }
  
  def read()={
        val a = dbConnection
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    
    
  val CONFIG_CYCLE = "DBBFMSXR.CONFIG_CYCLE"

  
   println(spark.read.jdbc(a.url, CONFIG_CYCLE, a.properties).select("EFFC_DATE", "DLET_DATE","CYCLE_ID").count())



    }
    
  
}
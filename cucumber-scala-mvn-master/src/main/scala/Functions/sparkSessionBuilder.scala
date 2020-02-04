package Functions

import org.apache.spark.sql.SparkSession

class sparkSessionBuilder {
  def getOrCreateSparkSession(): SparkSession ={
    SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", 4)
//      .config("spark.eventLog.enabled",true)
//      .config("spark.eventLog.dir","file:/C:/Users/SairamanKumar/logs")
      .appName("CLTwithCalculationReadwritecsvOfficeNetwork")
      .getOrCreate()
  }
}

package Calculate
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
import Joins.CLT
import Functions.{ConfigCycle,CustPCT,ReadTable}
class CLTNoRev {
  
  
  val configCycle = new ConfigCycle
  val custPCT = new CustPCT
  val clt = new CLT
  val readTable = new ReadTable
  val custPCT_NoRev= new CustPCT_NoRev
  
   def MST (): DataFrame ={
    val REQ_IMT_ID = 230
    val EFFC_DATE_SQL = configCycle.getEffcDate("ITT_CURRENT_CYCLE")
    val DF_TARGET_ACCOUNT = clt.joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL,REQ_IMT_ID)
    val DF_CUST_PCT_NOREV=custPCT_NoRev.calculate_CUS_PCT_NOREV().drop("TARGET_AMOUNT","GP_TARGET_AMOUNT","ADJ_TARGET_AMOUNT","ADJ_GP_TARGET_AMOUNT","GT10_TARGET_AMOUNT","LT10_TARGET_AMOUNT","ADJ_CLOUD_TARGET_AMOUNT")


   val DF_MST= DF_TARGET_ACCOUNT.join(DF_CUST_PCT_NOREV,Seq("TARGET_ACCOUNT_ID","BRAND_SUB_SUBGROUP_ID","ITT_ATTRIB_VALUE_ID_TGT")).withColumnRenamed("ZERO_REV_FACTOR","CUST_ACCT_PCT")

   
    DF_MST

  }


  
}
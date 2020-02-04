package Functions
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
class Covref {
  
  
  
   val readTable = new ReadTable
   val cset = new CSET

   
  def getValidCoveref(EFFC_DATE_SQL: Date): DataFrame = {



    val STRING_FMST_O_ITT_COV_PLAN_CONFIG = "FMST_O_ITT_COV_PLAN_CONFIG"

    val DF_FMST_O_ITT_COV_PLAN_CONFIG = readTable.readTableToDF(STRING_FMST_O_ITT_COV_PLAN_CONFIG)

   DF_FMST_O_ITT_COV_PLAN_CONFIG
      .select("CUSTOMER_SET_ID","ITT_COVREF_ID","EFFC_DATE","APPR_STATUS")
      .where("APPR_STATUS = 'Approved'")
      .where(DF_FMST_O_ITT_COV_PLAN_CONFIG("EFFC_DATE")===EFFC_DATE_SQL)
      .drop("APPR_STATUS").join(cset.getValidCSET(),Seq("CUSTOMER_SET_ID"))
      .drop("EFFC_DATE","DLET_DATE")
  }

}
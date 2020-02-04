package Functions
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}

class CustPCT {
  
         val readTable = new ReadTable

  
  
  def getCustPCT(EFFC_DATE_SQL: Date, REQ_IMT_ID: Int): DataFrame ={
    val STRING_FMST_R_ITT_CUST_PCT = "FMST_R_ITT_CUST_PCT"
    val DF_FMST_R_ITT_CUST_PCT = readTable.readTableToDF(STRING_FMST_R_ITT_CUST_PCT)
    DF_FMST_R_ITT_CUST_PCT
      .where(DF_FMST_R_ITT_CUST_PCT("EFFC_DATE")===EFFC_DATE_SQL)
      .where(DF_FMST_R_ITT_CUST_PCT("IMT_ID")===REQ_IMT_ID)
      .drop("EFFC_DATE","DLET_DATE")
  }

}
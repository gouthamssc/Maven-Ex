package Functions
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
class Cust {
  
                 val readTable = new ReadTable

  
  def getValidCust(REQ_IMT_ID: Int): DataFrame ={
    val STRING_FMST_O_ITT_CUST = "FMST_O_ITT_CUST"
    val DF_FMST_O_ITT_CUST=readTable.readTableToDF(STRING_FMST_O_ITT_CUST)

    DF_FMST_O_ITT_CUST
      .where("APPR_STATUS = 'Approved'")
      .where(DF_FMST_O_ITT_CUST("IMT_ID")===REQ_IMT_ID)
      .select("ITT_CUST_ID","IMT_ID","SAP_CUST_NBR","CI_CUST_NBR","CUSTNUM","CTRYNUM","CUST_NAME","COV_TYPE","COV_ID","COV_NAME")

  }
  
}
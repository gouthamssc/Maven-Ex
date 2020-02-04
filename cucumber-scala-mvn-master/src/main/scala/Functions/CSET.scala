package Functions
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
import Joins.CLT


class CSET {
  
  
  
        val readTable = new ReadTable
        val clt = new CLT

  
  def getValidCSET(): DataFrame ={


    val STRING_FMST_O_ITT_CUSTOMER_SET = "FMST_O_ITT_CUSTOMER_SET"

    val DF_FMST_O_ITT_CUSTOMER_SET = readTable.readTableToDF(STRING_FMST_O_ITT_CUSTOMER_SET)

    DF_FMST_O_ITT_CUSTOMER_SET.select("CUST_SET_NAME","IMT_ID","SUBMARKET_ID","CUSTOMER_SET_ID").join(clt.getRegisteredSubmarket(),Seq("IMT_ID","SUBMARKET_ID")).drop("SUBMARKET_ID")

  }
}
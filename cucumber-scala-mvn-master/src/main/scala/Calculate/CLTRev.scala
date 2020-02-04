package Calculate
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
import Joins.{CLT}
import Functions.ConfigCycle
class CLTRev {
  
  val clt = new CLT
    val configCycle = new ConfigCycle

  def calculateCLTREV(): DataFrame = {
    val EFFC_DATE_SQL = configCycle.getEffcDate("ITT_CURRENT_CYCLE")
    val REQ_IMT_ID = 230
    val DF_CLT_REV = clt.joinTargetCustwithBMDIVPCT(EFFC_DATE_SQL,REQ_IMT_ID)


    DF_CLT_REV
  }
}
package Calculate
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
import Joins.CLT
import Functions.{ConfigCycle,CustPCT,ReadTable}

class CustPCT_NoRev {
  
  
   val configCycle = new ConfigCycle
  val custPCT = new CustPCT
  val clt = new CLT
  val readTable = new ReadTable

    def calculate_CUS_PCT_NOREV(): DataFrame = {

    val REQ_IMT_ID = 230
    val EFFC_DATE_SQL = configCycle.getEffcDate("ITT_CURRENT_CYCLE")
    val vld_trgt = clt.joinValidAccountLevelTargetwithModelingVersion(EFFC_DATE_SQL, REQ_IMT_ID)
    val No_rev_cm = clt.joinCustwithTargetAccountCMRMapping( REQ_IMT_ID)

    val No_PCT=clt.getCustPCTNoRev()
    //  FMST_R_ITT_CUST_PCT

    val No_pct_j_vld_trgt = No_PCT.join(vld_trgt, Seq("TARGET_ACCOUNT_ID", "BRAND_SUB_SUBGROUP_ID", "ITT_ATTRIB_VALUE_ID_TGT", "IMT_ID"))

    val CMR_MAP_PCT = No_pct_j_vld_trgt.join(No_rev_cm, Seq("TARGET_ACCOUNT_ID"))


    val STRING_FMST_O_ITT_CUST = "FMST_O_ITT_CUST"
    val DF_FMST_O_ITT_CUST = readTable.readTableToDF(STRING_FMST_O_ITT_CUST).select("SAP_CUST_NBR", "CI_CUST_NBR", "CUSTNUM", "CTRYNUM", "CUST_NAME", "COV_TYPE", "COV_ID", "COV_NAME", "ITT_CUST_ID").where("APPR_STATUS = 'Approved'")

    val STRING_FMST_R_ITT_BMDIV_PCT = "FMST_R_ITT_BMDIV_PCT"
    val DF_FMST_R_ITT_BMDIV_PCT = readTable.readTableToDF(STRING_FMST_R_ITT_BMDIV_PCT).select("BMDIV_ID", "MODELING_LVL_PCT", "BRAND_SUB_SUBGROUP_ID", "ITT_ATTRIB_VALUE_ID_TGT")

    val VALID_CUST = CMR_MAP_PCT.join(DF_FMST_O_ITT_CUST, Seq("ITT_CUST_ID"), "inner")


    //  val var6 = var5.printSchema()
    val CUS_PCT_NOREV = VALID_CUST.join(DF_FMST_R_ITT_BMDIV_PCT, Seq("BRAND_SUB_SUBGROUP_ID", "ITT_ATTRIB_VALUE_ID_TGT"))

    CUS_PCT_NOREV
  }
}
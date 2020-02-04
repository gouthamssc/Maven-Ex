package Functions
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
class BMDivPCT {
  
                        val readTable = new ReadTable

   def getBMDivPCT(): DataFrame ={
     

    val STRING_FMST_R_ITT_BMDIV_PCT = "FMST_R_ITT_BMDIV_PCT"
    val DF_FMST_R_ITT_BMDIV_PCT=readTable.readTableToDF(STRING_FMST_R_ITT_BMDIV_PCT)

    DF_FMST_R_ITT_BMDIV_PCT.select("BMDIV_ID","MODELING_LVL_PCT","BRAND_SUB_SUBGROUP_ID","ITT_ATTRIB_VALUE_ID_TGT").withColumn("Mark",lit(1))
  }
  
  
}
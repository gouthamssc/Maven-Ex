package Functions
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
class CountryRollUpGeo {
  
               val readTable = new ReadTable

  def getCountryRollUpGeo(): DataFrame = {

    


    val STRING_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO = "FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO"

    val DF_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO = readTable.readTableToDF(STRING_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO)

    DF_FMSV2_O_ITT_CTRY_ROLLUP_TO_GEO
  }
}
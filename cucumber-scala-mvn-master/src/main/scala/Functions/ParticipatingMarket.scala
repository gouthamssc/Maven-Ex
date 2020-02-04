package Functions
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
class ParticipatingMarket {
                 val readTable = new ReadTable
                 
                 
def getParticipatingMarket(): DataFrame = {

    


    val STRING_FMSV1_O_ITT_PARTICIPATING_MRKT = "FMSV1_O_ITT_PARTICIPATING_MRKT"

    val DF_FMSV1_O_ITT_PARTICIPATING_MRKT = readTable.readTableToDF(STRING_FMSV1_O_ITT_PARTICIPATING_MRKT)

    DF_FMSV1_O_ITT_PARTICIPATING_MRKT
  }
}






  

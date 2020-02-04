package polymorphism

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import Functions.ReadTable

class poly{

def getAttributeValueToDF(selectColumnName: String,conditionColumnName: String, conditionColumnValue: String, castType: String = null):DataFrame = {
     
  
    val readTable = new ReadTable

  val STRING_FMST_R_ITT_ATTRIB_VALUE = "FMST_R_ITT_ATTRIB_VALUE"

    val DF_FMST_R_ITT_ATTRIB_VALUE = readTable.readTableToDF(STRING_FMST_R_ITT_ATTRIB_VALUE)

    if(castType==null){
      DF_FMST_R_ITT_ATTRIB_VALUE
        .select(DF_FMST_R_ITT_ATTRIB_VALUE(selectColumnName))
        .where(""+conditionColumnName+" = '"+conditionColumnValue+"'")
    }

    else {
      DF_FMST_R_ITT_ATTRIB_VALUE
        .select(DF_FMST_R_ITT_ATTRIB_VALUE(selectColumnName).cast(castType))
        .where(""+conditionColumnName+" = '"+conditionColumnValue+"'")
    }

}



 def main(args: Array[String]): Unit = {
  println()
   
   getAttributeValueToDF("ATTRIB_VALUE_DESC","ATTRIB_VALUE_CD"," ")  
   
}

}
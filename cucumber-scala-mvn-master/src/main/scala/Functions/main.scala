package Functions

import java.sql.Date


object main {
  
  
  
  def  main(args: Array[String]): Unit = {
    
    
   val cset = new CSET
   
   cset.getValidCSET().explain()
    
    
//    val modellingversion = new ModellingVersion
////        val targetaccount = new TargetAccount()
////val coverf= new Covref()
////    val DF_FMST_R_ITT_ATTRIB_VALUE_CURRENT_CYCLE = getattributeValue.getAttributeValueToDF("ATTRIB_VALUE_DESC","ATTRIB_VALUE_CD","ITT_CURRENT_CYCLE","int")
////    
////    DF_FMST_R_ITT_ATTRIB_VALUE_CURRENT_CYCLE.show()
//        
//        
////        requestTracking.getRequestTrackingID(RequestTypeID, RequestStatusID, "Approved")
//
////        val EFFC_DATE = "2020-01-01"
////        val EFFC_DATE_SQL = Date.valueOf(EFFC_DATE)
////        coverf.getValidCoveref(EFFC_DATE_SQL).printSchema()
////        
////        targetaccount.getTargetAccountID(EFFC_DATE_SQL, 230).printSchema()
//        
//    
//    modellingversion.getModellingVersion(230).printSchema()
  }
}
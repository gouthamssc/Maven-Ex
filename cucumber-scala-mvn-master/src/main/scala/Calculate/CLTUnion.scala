package Calculate
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset}
import java.sql.Date
import org.apache.spark.sql.functions.{current_timestamp, lit, when}
import Joins.CLT
import Functions.{ConfigCycle,CustPCT,ReadTable}

class CLTUnion {
  
  val cLTNoRev= new CLTNoRev
  val DF_cLT_NOREV =  cLTNoRev.MST

  val cLTRev= new CLTRev
  val DF_cLT_REV =  cLTRev.calculateCLTREV

  
  val DF_CLT = DF_cLT_REV.union(DF_cLT_NOREV)
  
  
    DF_CLT
    .withColumn("TARGET_AMOUNT",
      when(DF_CLT("TARGET_AMOUNT").isNull || DF_CLT("CUST_ACCT_PCT").isNull || DF_CLT("MODELING_LVL_PCT").isNull , 0)
        .otherwise(DF_CLT("TARGET_AMOUNT")*DF_CLT("CUST_ACCT_PCT")*DF_CLT("MODELING_LVL_PCT")))
    .withColumn("GP_TARGET_AMOUNT",
      when(DF_CLT("GP_TARGET_AMOUNT").isNull || DF_CLT("CUST_ACCT_PCT").isNull || DF_CLT("MODELING_LVL_PCT").isNull , 0)
        .otherwise(DF_CLT("GP_TARGET_AMOUNT")*DF_CLT("CUST_ACCT_PCT")*DF_CLT("MODELING_LVL_PCT")))
    .withColumn("ADJ_TARGET_AMOUNT",
      when(DF_CLT("ADJ_TARGET_AMOUNT").isNull || DF_CLT("CUST_ACCT_PCT").isNull || DF_CLT("MODELING_LVL_PCT").isNull , 0)
        .otherwise(DF_CLT("ADJ_TARGET_AMOUNT")*DF_CLT("CUST_ACCT_PCT")*DF_CLT("MODELING_LVL_PCT")))
    .withColumn("ADJ_GP_TARGET_AMOUNT",
      when(DF_CLT("ADJ_GP_TARGET_AMOUNT").isNull || DF_CLT("CUST_ACCT_PCT").isNull || DF_CLT("MODELING_LVL_PCT").isNull , 0)
        .otherwise(DF_CLT("ADJ_GP_TARGET_AMOUNT")*DF_CLT("CUST_ACCT_PCT")*DF_CLT("MODELING_LVL_PCT")))
    .withColumn("GT10_TARGET_AMOUNT",
      when(DF_CLT("GT10_TARGET_AMOUNT").isNull || DF_CLT("CUST_ACCT_PCT").isNull || DF_CLT("MODELING_LVL_PCT").isNull , 0)
        .otherwise(DF_CLT("GT10_TARGET_AMOUNT")*DF_CLT("CUST_ACCT_PCT")*DF_CLT("MODELING_LVL_PCT")))
    .withColumn("LT10_TARGET_AMOUNT",
      when(DF_CLT("LT10_TARGET_AMOUNT").isNull || DF_CLT("CUST_ACCT_PCT").isNull || DF_CLT("MODELING_LVL_PCT").isNull , 0)
        .otherwise(DF_CLT("LT10_TARGET_AMOUNT")*DF_CLT("CUST_ACCT_PCT")*DF_CLT("MODELING_LVL_PCT")))
    .withColumn("ADJ_CLOUD_TARGET_AMOUNT",
      when(DF_CLT("ADJ_CLOUD_TARGET_AMOUNT").isNull || DF_CLT("CUST_ACCT_PCT").isNull || DF_CLT("MODELING_LVL_PCT").isNull , 0)
      .otherwise(DF_CLT("ADJ_CLOUD_TARGET_AMOUNT")*DF_CLT("CUST_ACCT_PCT")*DF_CLT("MODELING_LVL_PCT")))
    .withColumn("APPR_STATUS",lit("Approved"))
    .withColumn("LAST_ACT_SYS_CD",lit("FMS#LCLT"))
    .withColumn("LAST_ACT_USER_ID",lit(0))
    .withColumn("CREATE_TIMESTAMP",lit(current_timestamp()))
    .withColumn("LAST_UPT_TIME",lit(current_timestamp()))
    .drop("CUST_PCT_ID","CUST_ACCT_PCT","MODELING_LVL_PCT")
  
}
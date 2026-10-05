
# Databricks notebook source
from src.utils.kafkaPublishUtils import publishMessage

# COMMAND ----------

dbutils.widgets.text("env", 'prod')
env = dbutils.widgets.get("env")
print(f"Running Policy Fund table for Env: {env}")

# COMMAND ----------

df = spark.sql(f"""SELECT (SELECT MAX(ValuationDate) FROM data2_enhanced_{env}.csdm.italicdata_dl) AS max_enhanced,(SELECT MAX(LastValueDate) FROM  data2_raw_{env}.uls.italicdata) AS max_raw""")

row = df.collect()[0]
if row["max_enhanced"] == row["max_raw"]:
    print("Condition satisfied stopping notebook")
    dbutils.notebook.exit("SKIPPED")
print("Condition not satisfied continuing execution")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS data2_enhanced_{env}.csdm.italicdata_dl")

spark.sql(f""" CREATE TABLE data2_enhanced_{env}.csdm.italicdata_dl as  select 
    policynumber,
    fund,
    allocatpercent,
    LastUnitPrice,
    UnitOwned,
    currentvalue,
    LastValueDate as ValuationDate,
    SMART,
    AAA,
    fundSwitches,
    redirections,
    current_timestamp() as createdAt,
    current_timestamp() as updatedAt
from data2_raw_{env}.uls.italicdata""")

# COMMAND ----------


spark.sql(f"DROP TABLE IF EXISTS data2_enhanced_{env}.csdm.topup_funds_dl")

spark.sql(f"""CREATE TABLE data2_enhanced_{env}.csdm.topup_funds_dl as
(
  with fundalloc as (
    SELECT
      c1.*
    FROM
      data2_raw_{env}.uls.coveragefundalloc c1
        inner join (
          SELECT
            policynumber,
            max(AllocatEffDate) AllocatEffDate
          FROM
            data2_raw_{env}.uls.coveragefundalloc
          WHERE
            companycode = '8026'
            and AllocatType = '5'
          GROUP BY
            policynumber
        ) c2
          ON c1.policynumber = c2.policynumber
          and c1.AllocatEffDate = c2.AllocatEffDate
    WHERE
      c1.AllocatType = '5'
  )
  SELECT
    POLICY_NO as policyNo,
    fund_code as code,
    ALLOCATION as allocPercent,
    last_unit_price as unitPrice,
    SUM(unit_owned) units,
    NAV_DATE as navDate,
    cast(null as boolean) as isSmart,
    cast(null as boolean) as isAAA,
    cast(null as int) as switchCount,
    cast(null as int) as redirectionCount,
    'TopUpFund' as type,
    cast(null as string) as createdBy,
    cast(null as string) as updatedBy,
    current_timestamp() as createdAt,
    current_timestamp() as updatedAt,
    cast(null as string) as attributes
  FROM
    (
      SELECT
        IA.POLICYNUMBER POLICY_NO,
        IA.FUNDCODE FUND_CODE,
        COALESCE(CF.Allocatpercent, 0) ALLOCATION,
        FP.BID LAST_UNIT_PRICE,
        IA.NOFUNITSOWNED UNIT_OWNED,
        FP.BID * IA.NOFUNITSOWNED CURRENT_VALUE,
        FP.VALUATIONDATE NAV_DATE
      FROM
        data2_raw_{env}.uls.investmentaccount IA
          left join fundalloc CF
            on ia.policynumber = cf.policynumber
            and ia.fundcode = cf.fundcode
          LEFT OUTER JOIN data2_raw_{env}.uls.FUNDPRICE FP
            ON IA.FUNDCODE = FP.FUNDCODE
      WHERE
        IA.companycode = '8026'
        and IA.investmentacct = 'T'
        AND FP.PRICINGDATE = (
          SELECT
            MAX(FP2.PRICINGDATE)
          FROM
            data2_raw_{env}.uls.FUNDPRICE FP2
          WHERE
            FP2.COMPANYCODE = FP.COMPANYCODE
            AND FP2.FUNDCODE = FP.FUNDCODE
            AND FP2.PRICINGDATE <= GETDATE()
        )
    ) a
  GROUP BY
    POLICY_NO,
    fund_code,
    ALLOCATION,
    last_unit_price,
    NAV_DATE
)""")

# COMMAND ----------

spark.sql(f"drop table if exists data2_enhanced_{env}.csdm.italicdata_unclaimed")

spark.sql(f"""create table data2_enhanced_{env}.csdm.italicdata_unclaimed as
WITH temp1 AS (
  SELECT
    ia.policynumber,
    ia.FundCode AS Fund,
    100 AS allocatpercent,
    fp.Bid AS LastUnitPrice,
    ia.NofUnitsOwned AS UnitOwned,
    fp.Bid * ia.NofUnitsOwned AS CurrentValue,
    fp.ValuationDate,
    cast(null as boolean) as isSmart,
    cast(null as boolean) as isAAA,
    cast(null as int) as switchCount,
    cast(null as int) as redirectionCount
  FROM
    data2_raw_{env}.uls.fundprice fp
      JOIN data2_raw_{env}.uls.investmentaccount_unclaimed ia
        ON ia.FundCode = fp.FundCode
  WHERE
    ia.CompanyCode = '8026'
    AND fp.PricingDate = (
      SELECT
        MAX(fp2.PricingDate)
      FROM
        data2_raw_{env}.uls.fundprice fp2
      WHERE
        fp2.CompanyCode = fp.CompanyCode
        AND fp2.FundCode = fp.FundCode
    )
)
select
  policynumber as policyNo,
  fund as code,
  CAST(100.00 AS DECIMAL(16, 7)) AS allocPercent,
  cast(LastUnitPrice as decimal(17,8)) as unitPrice,
  cast(UnitOwned as decimal(19,6)) as units,
  ValuationDate as navDate,
  isSmart,
  isAAA,
  switchCount,
  redirectionCount,
  'Unclaimed' as type,
  cast(null as string) as createdBy,
  cast(null as string) as updatedBy,
  current_timestamp() as createdAt,
  current_timestamp() as updatedAt,
  cast(null as string) as attributes
from
  temp1
group by
  policynumber,
  fund,
  LastUnitPrice,
  UnitOwned,
  ValuationDate,
  isSmart,
  isAAA,
  switchCount,
  redirectionCount""")

# COMMAND ----------

spark.sql(f"drop table if exists data2_target_{env}.csdm.policyFund")

spark.sql(f"""create table data2_target_{env}.csdm.policyFund
(
  id bigint GENERATED ALWAYS AS IDENTITY,
  policyNo string,
  code string,
  allocPercent decimal(16,4),
  unitPrice decimal(17,8),
  units decimal(19,6),
  navDate date,
  isSmart boolean,
  isAAA boolean,
  switchCount int,
  redirectionCount int,
  type string,
  createdBy string,
  updatedBy string,
  createdAt timestamp,
  updatedAt timestamp,
  attributes string,
  is_deleted boolean
)""")


spark.sql(f"""insert into data2_target_{env}.csdm.policyFund (policyNo,code,allocPercent,unitPrice,units,navDate,isSmart,isAAA,switchCount,redirectionCount,type,createdBy,updatedBy,createdAt,updatedAt,attributes,is_deleted)
select
  policyNo,
  code,
  cast(allocPercent as decimal(16,4)) as allocPercent,
  cast(unitPrice as decimal(17,8)) as unitPrice,
  cast(units as decimal(19,6)) as units,
  navDate,
  isSmart,
  isAAA,
  switchCount,
  redirectionCount,
  type,
  createdBy,
  updatedBy,
  createdAt,
  updatedAt,
  attributes,
  false as is_deleted
from
  data2_enhanced_{env}.csdm.topup_funds_dl
union all
select
  policynumber as policyNo,
  fund as code,
  cast(allocatpercent as decimal(16,4)) as allocPercent,
  cast(LastUnitPrice as  decimal(17,8)) as unitPrice,
  cast(UnitOwned as decimal(19,6)) as units,
  ValuationDate as navDate,
  cast(SMART as boolean) as isSmart,
  cast(AAA as boolean) as isAAA,
  cast(fundSwitches as int) as switchCount,
  cast(redirections as int) as redirectionCount,
  'Fund' as type,
  cast(null as string) as createdBy,
  cast(null as string) as updatedBy,
  cast(null as timestamp) as createdAt,
  cast(null as timestamp) as updatedAt,
  cast(null as string) as attributes,
  false as is_deleted
from
  data2_enhanced_{env}.csdm.italicdata_dl
union all
select policyNo,code,allocPercent,unitPrice,units,navDate,isSmart,isAAA,switchCount,redirectionCount,type,createdBy,updatedBy,createdAt,updatedAt,attributes,false as is_deleted from data2_enhanced_{env}.csdm.italicdata_unclaimed
union all
select
  policyNo as policyNo,
  code as code,
  cast(null as decimal(16,4)) as allocPercent,
  cast(null as decimal(17,8)) as unitPrice,
  cast(null as decimal(19,6)) as units,
  cast(null as date) as navDate,
  cast(null as boolean) as isSmart,
  cast(null as boolean) as isAAA,
  cast(null as int) as switchCount,
  cast(null as int) as redirectionCount,
  'Fund' as type,
  cast("GPL" as string) as createdBy,
  cast("GPL" as string) as updatedBy,
  current_timestamp() as createdAt,
  current_timestamp() as updatedAt,
  to_json(named_struct(
  'masterPolicyNo', masterPolicyNo,
  'contributionAmount',contributionAmount,
  'adminFee',adminFee,
  'serviceTax',serviceTax
)) as attributes,
  false as is_deleted
  from
(
with distinct_fund AS (
    select  distinct coi as policyNO , PolicyNumber as masterPolicyNo , FundCode as code from  data2_raw_{env}.glimpse.tbl_fund_investment_gpl  where InvestmentType = 'CN' or InvestmentType = 'FEE' or InvestmentType = 'GST'
 ),
 contribution_fund AS (
       select  sum(Amount_EE + Amount_ER) as contributionAmount , coi as policyNo , FundCode as code ,PolicyNumber AS masterPolicyNo , current_timestamp as createdAt , current_timestamp as updatedAt, cast("GPL" as string) as createdBy, cast("GPL" as string) as updatedBy  from  data2_raw_{env}.glimpse.tbl_fund_investment_gpl where InvestmentType = 'CN' group by PolicyNumber,COI,FundCode
 ),
admin_charge AS (
      select  sum(Amount_EE + Amount_ER) as adminFee , coi as policyNo , FundCode as code ,PolicyNumber AS masterPolicyNo , current_timestamp as createdAt , current_timestamp as updatedAt, cast("GPL" as string) as createdBy, cast("GPL" as string) as updatedBy  from  data2_raw_{env}.glimpse.tbl_fund_investment_gpl where InvestmentType = 'FEE' group by PolicyNumber,COI,FundCode
),
serviceTax AS (
      select  sum(Amount_EE + Amount_ER) as serviceTax , coi as policyNo , FundCode as code ,PolicyNumber AS masterPolicyNo , current_timestamp as createdAt , current_timestamp as updatedAt, cast("GPL" as string) as createdBy, cast("GPL" as string) as updatedBy  from  data2_raw_{env}.glimpse.tbl_fund_investment_gpl where InvestmentType = 'GST' group by PolicyNumber,COI,FundCode
)
select
distinct_fund.masterPolicyNo as masterPolicyNo,
distinct_fund.code as code,
distinct_fund.policyno as policyNo,
contributionAmount,
adminFee,
serviceTax
 from distinct_fund
left join contribution_fund on distinct_fund.policyno = contribution_fund.policyno and distinct_fund.code = contribution_fund.code and distinct_fund.masterPolicyNo = contribution_fund.masterPolicyNo
left join admin_charge on distinct_fund.policyno = admin_charge.policyno and distinct_fund.code = admin_charge.code and distinct_fund.masterPolicyNo = admin_charge.masterpolicyno left join serviceTax on distinct_fund.policyno = serviceTax.policyno and distinct_fund.code = serviceTax.code and distinct_fund.masterPolicyNo = serviceTax.masterPolicyNo where contributionamount is not null or adminFee is not null or serviceTax is not null)
""")

# COMMAND ----------

spark.sql(f"truncate table data2_target_{env}.cdm.policyFund")
spark.sql(f"insert into data2_target_{env}.cdm.policyFund select * from data2_target_{env}.csdm.policyFund")

# COMMAND ----------

from pg_utils.utils import *
from pyspark.sql.functions import lit
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import *
import psycopg2
def executeQuery(query):
    with psycopg2.connect(
        user=pg_obj.user,
        password=pg_obj.password,
        host=pg_obj.host,
        port=pg_obj.port,
        database=pg_obj.database,
    ) as connection:
        with connection.cursor() as cursor:
            try:
                connection.autocommit = False
                cursor.execute(query)
                connection.commit()
                print("All queries executed successfully and transaction committed")
            except Exception as e:
                print(f"Error: {e}")
                connection.rollback()
                print("Transaction rolled back")

# COMMAND ----------


table_name = "PolicyFund"
pg_schema = 'cdm_dev' if env == 'dev' else 'cdm'
pg_full_load_table = f'{pg_schema}."{table_name}FullLoad"'
pg_table = f'{pg_schema}."{table_name}"'
df = spark.read.table(f"data2_target_{env}.cdm.policyfund")
pg_obj = PgPush(env)
query = f"""truncate table {pg_full_load_table}"""
executeQuery(query)
df.repartition(8).write.format("jdbc").mode("append").options(
    url=pg_obj.jdbc_url,
    dbtable=pg_full_load_table,
    user=pg_obj.user,
    password=pg_obj.password,
    driver=pg_obj.driver,
    stringtype="unspecified",
    prepareThreshold=0,
    batchsize=50000,
    isolationLevel="NONE",
    reWriteBatchedInserts="true"
).save()

# COMMAND ----------

# Reset identity sequence to avoid duplicate key conflict
reset_seq_query = f"""SELECT setval(pg_get_serial_sequence('{pg_table}', 'id'), COALESCE((SELECT MAX(id) FROM {pg_table}), 0))"""
executeQuery(reset_seq_query)

query = f"""MERGE INTO {pg_table}  AS target
    using (SELECT * FROM {pg_full_load_table} WHERE "createdBy" IS NULL)  AS source
    ON target."policyNo" = source."policyNo"
    AND target.code = source.code and target."type"  = source."type" 

    WHEN MATCHED THEN
    UPDATE SET
        "allocPercent" = source."allocPercent",
        "unitPrice" = source."unitPrice",
        units = source.units,
        "navDate" = source."navDate",
        "isSmart" = source."isSmart",
        "isAAA" = source."isAAA",
        "switchCount" = source."switchCount",
        "redirectionCount" = source."redirectionCount",
        "updatedBy" = source."updatedBy",
        "updatedAt" =source."updatedAt",
        "is_deleted" = source."is_deleted"

    WHEN NOT MATCHED THEN 
    INSERT (
        "policyNo", code, "allocPercent", "unitPrice", units, "navDate",
        "isSmart", "isAAA", "switchCount", "redirectionCount", type,
        "createdBy", "updatedBy", "createdAt", "updatedAt", "is_deleted"
    )
    VALUES (
        source."policyNo", source.code, source."allocPercent", source."unitPrice",
        source.units, source."navDate", source."isSmart", source."isAAA", source."switchCount",
        source."redirectionCount", source.type, source."createdBy", source."updatedBy",
        source."createdAt", source."updatedAt", source."is_deleted"
    );"""
print(f"Loading into postgres {pg_table} table")
executeQuery(query)

# COMMAND ----------


query = f"""MERGE INTO {pg_table}  AS target
    using (SELECT * FROM {pg_full_load_table} WHERE "createdBy" IS not null)  AS source
    ON target."policyNo" = source."policyNo"
    AND target.code = source.code and target."type"  = source."type" 
    AND CAST(target.attributes AS jsonb)->>'masterPolicyNo' = CAST(source.attributes AS jsonb)->>'masterPolicyNo'
    
    WHEN MATCHED THEN
    UPDATE SET
        type = source.type,
        "allocPercent" = source."allocPercent",
        "unitPrice" = source."unitPrice",
        units = source.units,
        "navDate" = source."navDate",
        "isSmart" = source."isSmart",
        "isAAA" = source."isAAA",
        "switchCount" = source."switchCount",
        "redirectionCount" = source."redirectionCount",
        "updatedBy" = source."updatedBy",
        "updatedAt" = current_timestamp,
        "is_deleted" = source."is_deleted"

    WHEN NOT MATCHED THEN 
    INSERT (
        "policyNo", code, "allocPercent", "unitPrice", units, "navDate",
        "isSmart", "isAAA", "switchCount", "redirectionCount", type,
        "createdBy", "updatedBy", "createdAt", "updatedAt","attributes", "is_deleted"
    )
    VALUES (
        source."policyNo", source.code, source."allocPercent", source."unitPrice",
        source.units, source."navDate", source."isSmart", source."isAAA", source."switchCount",
        source."redirectionCount", source.type, source."createdBy", source."updatedBy",
        current_timestamp, current_timestamp,CAST(source."attributes" AS jsonb), source."is_deleted"
    );"""
print(f"Loading into postgres {pg_table} table")
executeQuery(query)


# COMMAND ----------


query = f"""UPDATE {pg_table} t
    SET "is_deleted" = true
    WHERE NOT EXISTS (
        SELECT 1
        FROM {pg_full_load_table} s
        WHERE t."policyNo" = s."policyNo"
        AND t.code = s.code
    );"""
print(f"updating is_deleted flag in  {pg_table} table")
executeQuery(query)

# COMMAND ----------

kafka_bootstrap_servers = '172.23.96.217:9092'if env == 'dev' else "172.23.4.20:9092,172.23.4.21:9092,172.23.4.23:9092"
kafka_topic = f"uat.au.cdm.policyfund" if env == 'dev' else f'production.au.cdm.policyfund'
table_name = f'data2_target_{env}.cdm.policyfund'
publishMessage(spark.read.table(table_name), kafka_bootstrap_servers, kafka_topic, table_name)

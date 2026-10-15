{'axis': ('5630382_17','5630382_24','5630382_44','4570276','5630382','5630382_43')}


from src.utils.kafkaPublishUtils import publishMessage
from pyspark.sql.functions import col,lit,to_json, struct, col, date_format
import ast,hashlib
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
from functools import reduce

dbutils.widgets.text("bank_event", "")
dbutils.widgets.text("env", 'prod')
try:
    bank_event = ast.literal_eval(dbutils.widgets.get("bank_event"))
    env = dbutils.widgets.get("env")
except TypeError as e:
    print(f"Error: {e}")


cdc_version = spark.sql(f"select version from (describe history data2_target_{env}.cdm.policyfund limit 20) where operation = 'WRITE'  LIMIT 1 OFFSET 1").first()[0]
print(cdc_version)

df2 = spark.sql(f"""select policyNo,code,allocpercent,unitprice,units from data2_target_{env}.cdm.policyfund version as of {cdc_version}""")


event_dfs = []
try:
    for bank in bank_event:
        timestmp = str(datetime.now())

        df1 = spark.sql(f"""
                            SELECT
                                pf.policyNo,
                                pf.code,
                                pf.allocPercent,
                                pf.unitPrice,
                                pf.units,
                                t.status
                            FROM data2_target_{env}.cdm.policyfund pf
                            INNER JOIN (
                                SELECT
                                    p.policyNo,
                                    p.status
                                FROM data2_target_{env}.csdm.policy p
                                JOIN (
                                    SELECT policyNo
                                    FROM data2_target_{env}.cdm.identity
                                    WHERE type = 'BANK'
                                    AND NULLIF(TRIM(idNo), '') IS NOT NULL
                                ) i
                                    ON p.policyNo = i.policyNo
                                WHERE agentCode IN (
                                    SELECT agentCode
                                    FROM data2_target_{env}.cdm.agent
                                    WHERE partnerCode IN {bank_event[bank]}
                                )
                                AND p.policyNo LIKE 'U%'
                                AND p.status = '20'
                            ) t
                                ON pf.policyNo = t.policyNo
                            """)


        compare_df = (
                        df1.alias("a")
                        .join(
                            df2.alias("b"),
                            (col("a.policyNo") == col("b.policyNo")) &
                            (col("a.code") == col("b.code")),
                            "inner"
                        )
                        .filter(
                            ~(col("a.allocPercent").eqNullSafe(col("b.allocPercent"))) |
                            ~(col("a.unitPrice").eqNullSafe(col("b.unitPrice"))) |
                            ~(col("a.units").eqNullSafe(col("b.units")))
                        )
                        .select(
                            col("a.policyNo"),
                            col("a.status")
                        )
                        .distinct()
                    )
        

        event_df = compare_df.select(
            lit(hashlib.md5(bank.encode()).hexdigest()).alias("id"),
            lit(f"{bank}FundChanges").alias("name"),
            lit("event.policy.master").alias("_service"),
            struct(
                col("policyNo").alias("policyNo"),
                col("status").alias("status"),
                lit(timestmp).alias("timestamp")
            ).alias("attributes"),
            lit("EVENT").alias("_type"),
            lit(f"{bank}FundChanges").alias("_operation"),
            lit("datalake").alias("source"),
            lit("policy.master").alias("type")
        )

        event_dfs.append(event_df)
    final_df = reduce(lambda x, y: x.unionByName(y),event_dfs)

except TypeError as e:
    print(f"Error: {e}")



kafka_bootstrap_servers = 'xyz'
kafka_topic = f"integration1.event.policy.master" 
table_name = f'data2_target_{env}.cdm.policyfund'
publishMessage(final_df, kafka_bootstrap_servers, kafka_topic, table_name)

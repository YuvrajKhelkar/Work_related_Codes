from src.utils.kafkaPublishUtils import publishMessage
from pyspark.sql import functions as F
import ast, hashlib
from datetime import datetime

# ---------- Parameters ----------
dbutils.widgets.text("bank_event", "")
dbutils.widgets.text("env", "prod")

bank_event = ast.literal_eval(dbutils.widgets.get("bank_event"))
env = dbutils.widgets.get("env")

if not bank_event:
    raise ValueError("bank_event is empty; nothing to publish")

# ---------- Previous version of policyfund (same logic as original) ----------
cdc_version = (
    spark.sql(f"""
        SELECT version
        FROM (DESCRIBE HISTORY data2_target_{env}.cdm.policyfund LIMIT 20)
        WHERE operation = 'WRITE'
        LIMIT 1 OFFSET 1
    """)
    .first()[0]
)
print(f"Comparing against version {cdc_version}")

baseline = spark.sql(f"""
    SELECT policyNo, code, allocPercent, unitPrice, units
    FROM data2_target_{env}.cdm.policyfund VERSION AS OF {cdc_version}
""")

current_funds = spark.sql(f"""
    SELECT policyNo, code, allocPercent, unitPrice, units
    FROM data2_target_{env}.cdm.policyfund
""")

# ---------- Bank -> partner mapping (tiny, broadcast) ----------
partner_rows = [
    (bank, partner)
    for bank, partners in bank_event.items()
    for partner in (partners if isinstance(partners, (list, tuple)) else [partners])
]
partner_map = spark.createDataFrame(partner_rows, "bank STRING, partnerCode STRING")

# ---------- Agents mapped to banks (small) ----------
bank_agents = (
    spark.table(f"data2_target_{env}.cdm.agent")
         .select("agentCode", "partnerCode")
         .join(F.broadcast(partner_map), "partnerCode")
         .select("agentCode", "bank")
         .distinct()
)

# ---------- Bank identities (filtered early; clustered on type/policyNo) ----------
bank_identity = (
    spark.table(f"data2_target_{env}.cdm.identity")
         .where((F.col("type") == "BANK") & (F.trim(F.col("idNo")) != ""))
         .select("policyNo")
)

# ---------- Qualifying policies, tagged with bank ----------
policies = (
    spark.table(f"data2_target_{env}.csdm.policy")
         .where(F.col("policyNo").startswith("U") & (F.col("status") == "20"))
         .select("policyNo", "status", "agentCode")
         .join(bank_agents, "agentCode")
         .join(bank_identity, "policyNo", "left_semi")
         .select("policyNo", "status", "bank")
)

# ---------- Policies whose fund values changed ----------
changed = (
    current_funds.alias("a")
        .join(baseline.alias("b"), ["policyNo", "code"], "inner")
        .where(
            ~F.col("a.allocPercent").eqNullSafe(F.col("b.allocPercent")) |
            ~F.col("a.unitPrice").eqNullSafe(F.col("b.unitPrice")) |
            ~F.col("a.units").eqNullSafe(F.col("b.units"))
        )
        .select("policyNo")
        .distinct()
)

# ---------- Changed policies that qualify, per bank ----------
result = (
    policies.join(changed, "policyNo", "left_semi")
            .select("policyNo", "status", "bank")
            .distinct()
)

# ---------- Build events (same shape as original) ----------
timestamp = str(datetime.now())

final_df = result.select(
    F.md5(F.col("bank").cast("binary")).alias("id"),
    F.concat(F.col("bank"), F.lit("FundChanges")).alias("name"),
    F.lit("event.policy.master").alias("_service"),
    F.struct(
        F.col("policyNo").alias("policyNo"),
        F.col("status").alias("status"),
        F.lit(timestamp).alias("timestamp"),
    ).alias("attributes"),
    F.lit("EVENT").alias("_type"),
    F.concat(F.col("bank"), F.lit("FundChanges")).alias("_operation"),
    F.lit("datalake").alias("source"),
    F.lit("policy.master").alias("type"),
)

# ---------- Publish ----------
kafka_bootstrap_servers = "xyz"
kafka_topic = "integration1.event.policy.master"
table_name = f"data2_target_{env}.cdm.policyfund"

publishMessage(final_df, kafka_bootstrap_servers, kafka_topic, table_name)

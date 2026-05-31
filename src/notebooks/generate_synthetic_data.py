# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Synthetic UK Insurance Claims Data
# MAGIC
# MAGIC Writes CSV files to the bronze landing Volume (simulates external ingestion).
# MAGIC Autoloader (Landing to Bronze pipeline) reads these files into Delta tables.
# MAGIC
# MAGIC **Business story — February 2026 UK Winter Storm:**
# MAGIC - Storm Freya sweeps across Yorkshire, the Midlands, and Somerset (Feb 3–17, 2026)
# MAGIC - 35% of all claims (42K) are storm-related — flooding, wind damage, fallen trees
# MAGIC - Storm claims show higher severity and 14% fraud rate vs 8% baseline
# MAGIC - Provides root-cause analysis, financial impact, and fraud detection signals
# MAGIC
# MAGIC **Geography:** England and Wales regions, UK postcodes, GB locale
# MAGIC
# MAGIC **Bad data injection (~3% per table)** targeting DQ rules in Bronze→Silver pipeline.

# COMMAND ----------
# MAGIC %pip install faker

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
import pandas as pd
from datetime import date

# COMMAND ----------

dbutils.widgets.text("bronze_catalog", "bronze_dev", "Bronze Catalog")
dbutils.widgets.text("bronze_schema",  "raw_claims",  "Bronze Schema")

CATALOG = dbutils.widgets.get("bronze_catalog")
SCHEMA  = dbutils.widgets.get("bronze_schema")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"

print(f"Landing path : {LANDING}")

# COMMAND ----------
# ── Row counts & bad data ratio ───────────────────────────────────────────────

N_PH  = 50_000
N_VH  = 55_000
N_POL = 60_000
N_CLM = 120_000
N_INC = 120_000
N_ASS = 180_000

BAD_RATIO = 0.03
N_BAD_PH  = int(N_PH  * BAD_RATIO)
N_BAD_VH  = int(N_VH  * BAD_RATIO)
N_BAD_POL = int(N_POL * BAD_RATIO)
N_BAD_CLM = int(N_CLM * BAD_RATIO)
N_BAD_INC = int(N_INC * BAD_RATIO)
N_BAD_ASS = int(N_ASS * BAD_RATIO)

STORM_RATIO      = 0.35
STORM_START_DAYS = (date(2026, 2, 3)  - date(1970, 1, 1)).days   # Storm Freya start
STORM_END_DAYS   = (date(2026, 2, 17) - date(1970, 1, 1)).days   # Storm Freya end
HIST_START_DAYS  = (date(2024, 1, 1)  - date(1970, 1, 1)).days
HIST_END_DAYS    = STORM_START_DAYS - 1

def to_str(df):
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])

# COMMAND ----------
# ── Faker pandas UDFs (en_GB locale) ─────────────────────────────────────────

@F.pandas_udf(StringType())
def udf_first_name(s: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker('en_GB')
    return pd.Series([fake.first_name() for _ in s])

@F.pandas_udf(StringType())
def udf_last_name(s: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker('en_GB')
    return pd.Series([fake.last_name() for _ in s])

@F.pandas_udf(StringType())
def udf_phone(s: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker('en_GB')
    return pd.Series([fake.phone_number() for _ in s])

@F.pandas_udf(StringType())
def udf_address(s: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker('en_GB')
    return pd.Series([fake.street_address() for _ in s])

@F.pandas_udf(StringType())
def udf_postcode(s: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker('en_GB')
    return pd.Series([fake.postcode() for _ in s])

@F.pandas_udf(StringType())
def udf_vin(s: pd.Series) -> pd.Series:
    from faker import Faker
    fake = Faker()
    return pd.Series([fake.vin() for _ in s])

@F.pandas_udf(StringType())
def udf_city_for_region(regions: pd.Series) -> pd.Series:
    import random
    cities = {
        "Greater London":     ["London", "Croydon", "Bromley", "Ealing", "Hackney", "Islington", "Southwark", "Lambeth"],
        "West Yorkshire":     ["Leeds", "Bradford", "Halifax", "Huddersfield", "Wakefield", "Dewsbury", "Keighley"],
        "West Midlands":      ["Birmingham", "Coventry", "Wolverhampton", "Solihull", "Walsall", "Dudley"],
        "Greater Manchester": ["Manchester", "Salford", "Bolton", "Stockport", "Oldham", "Rochdale", "Wigan"],
        "South Yorkshire":    ["Sheffield", "Doncaster", "Rotherham", "Barnsley"],
        "Merseyside":         ["Liverpool", "Birkenhead", "St Helens", "Bootle", "Southport"],
        "Hampshire":          ["Southampton", "Portsmouth", "Winchester", "Basingstoke", "Eastleigh"],
        "Essex":              ["Chelmsford", "Southend-on-Sea", "Colchester", "Basildon", "Harlow"],
        "Kent":               ["Maidstone", "Canterbury", "Rochester", "Dartford", "Folkestone"],
        "Lancashire":         ["Preston", "Blackpool", "Burnley", "Blackburn", "Lancaster"],
        "Somerset":           ["Taunton", "Bath", "Weston-super-Mare", "Yeovil", "Bridgwater"],
        "Nottinghamshire":    ["Nottingham", "Mansfield", "Worksop", "Newark-on-Trent"],
        "Devon":              ["Exeter", "Plymouth", "Torquay", "Barnstaple", "Newton Abbot"],
        "Tyne and Wear":      ["Newcastle upon Tyne", "Sunderland", "Gateshead", "South Shields"],
        "Bristol":            ["Bristol", "Clifton", "Bedminster", "Avonmouth"],
        "North Yorkshire":    ["York", "Harrogate", "Scarborough", "Northallerton"],
    }
    default = ["Reading", "Oxford", "Cambridge", "Norwich", "Leicester"]
    return pd.Series([random.choice(cities.get(r, default)) for r in regions])

@F.pandas_udf(StringType())
def udf_claim_desc(types: pd.Series) -> pd.Series:
    import random
    descs = {
        "weather":   ["Flood damage to engine and interior", "Storm caused windscreen shatter",
                      "Fallen tree branch damaged roof and bonnet", "Wind-driven debris struck vehicle",
                      "Floodwater submerged vehicle", "Storm Freya caused extensive bodywork damage"],
        "collision": ["Rear-end shunt at traffic lights", "Side-swipe on motorway",
                      "Collision at roundabout", "Hit parked car while reversing",
                      "Multi-vehicle incident on A-road"],
        "theft":     ["Vehicle stolen from car park", "Catalytic converter stolen overnight",
                      "Break-in and valuables stolen", "Vehicle found stripped",
                      "Keyless entry relay attack theft"],
        "vandalism": ["Keyed along both sides", "Tyres slashed overnight",
                      "Windscreen smashed", "Spray paint on bonnet", "Wing mirrors snapped off"],
        "fire":      ["Engine fire whilst driving", "Electrical fault caused fire",
                      "Vehicle fire in garage", "Arson suspected", "Fire due to fuel leak"],
    }
    return pd.Series([random.choice(descs.get(t, ["Damage reported"])) for t in types])

# COMMAND ----------
# ── 1. POLICYHOLDERS ─────────────────────────────────────────────────────────

# UK regions weighted toward Greater London and Yorkshire (storm-prone)
UK_REGIONS = (
    ["Greater London"]     * 22 +
    ["West Yorkshire"]     * 12 +   # storm-affected
    ["West Midlands"]      * 10 +
    ["Greater Manchester"] * 10 +
    ["South Yorkshire"]    * 8  +   # storm-affected
    ["Merseyside"]         * 6  +
    ["Hampshire"]          * 5  +
    ["Essex"]              * 5  +
    ["Kent"]               * 4  +
    ["Lancashire"]         * 4  +
    ["Somerset"]           * 4  +   # storm-affected
    ["Nottinghamshire"]    * 3  +
    ["Devon"]              * 3  +
    ["Tyne and Wear"]      * 2  +
    ["Bristol"]            * 1  +
    ["North Yorkshire"]    * 1
)
region_arr = F.array([F.lit(r) for r in UK_REGIONS])

ph_df = (
    spark.range(0, N_PH, numPartitions=8)
    .select(
        F.concat(F.lit("PH-"), F.lpad((F.col("id") + 1).cast("string"), 6, "0")).alias("policyholder_id"),
        udf_first_name(F.col("id")).alias("first_name"),
        udf_last_name(F.col("id")).alias("last_name"),
        F.date_sub(F.current_date(),
            F.greatest(F.lit(17 * 365), F.least(F.lit(80 * 365),
                (F.round(F.abs(F.randn(42) * 10 + 45)) * 365).cast(IntegerType())))
        ).alias("date_of_birth"),
        F.when(F.rand(1) < 0.50, "M").when(F.rand(2) < 0.97, "F").otherwise("Other").alias("gender"),
        F.concat(F.lower(udf_first_name(F.col("id"))), F.lit("."), F.lower(udf_last_name(F.col("id") + 1)),
            F.lit("@"), F.element_at(
                F.array(F.lit("gmail.com"), F.lit("hotmail.co.uk"), F.lit("outlook.com"), F.lit("yahoo.co.uk")),
                (F.abs(F.hash(F.col("id"))) % 4 + 1).cast(IntegerType()))
        ).alias("email"),
        udf_phone(F.col("id")).alias("phone"),
        udf_address(F.col("id")).alias("street_address"),
        F.element_at(region_arr, (F.abs(F.hash(F.col("id"))) % len(UK_REGIONS) + 1).cast(IntegerType())).alias("region"),
        udf_postcode(F.col("id")).alias("postcode"),
        # UK penalty points: 70% none, licence suspended at 12
        F.when(F.rand(3) < 0.70, 0).when(F.rand(4) < 0.85, 3).when(F.rand(5) < 0.95, 6)
         .otherwise((F.abs(F.hash(F.col("id"))) % 3 + 9).cast(IntegerType())).alias("driving_points"),
        F.greatest(F.lit(1), (F.abs(F.randn(6) * 6 + 18).cast(IntegerType()) + 2)).alias("driving_years"),
        # UK driving licence: 5 letters (surname) + 6 digits + 2 letters + 2 digits
        F.concat(F.upper(F.substring(udf_last_name(F.col("id")), 1, 5)),
                 F.lpad((F.abs(F.hash(F.col("id"))) % 900000 + 100000).cast("string"), 6, "0"),
                 F.upper(F.substring(udf_first_name(F.col("id")), 1, 2)),
                 F.lpad((F.abs(F.hash(F.col("id") + 7)) % 90 + 10).cast("string"), 2, "0")
        ).alias("licence_number"),
        F.lit("2024-01-01").alias("created_date"),
    )
    .withColumn("city", udf_city_for_region(F.col("region")))
)

bad_ph = (
    spark.range(N_PH, N_PH + N_BAD_PH, numPartitions=2)
    .select(
        F.when(F.col("id") % 3 == 0, F.lit(None))
         .otherwise(F.concat(F.lit("BAD-"), F.lpad((F.col("id")+1).cast("string"), 6, "0"))).alias("policyholder_id"),
        F.lit("Test").alias("first_name"),
        F.lit("BadRecord").alias("last_name"),
        F.when(F.col("id") % 3 == 1, F.lit("2021-06-15"))
         .otherwise(F.lit("1985-03-20")).alias("date_of_birth"),
        F.lit("M").alias("gender"),
        F.when(F.col("id") % 3 == 2, F.lit("invalidemail-missing-at-sign"))
         .otherwise(F.lit("test@example.co.uk")).alias("email"),
        F.lit("07700 900000").alias("phone"),
        F.lit("1 Test Street").alias("street_address"),
        F.lit("Greater London").alias("region"),
        F.lit("SW1A 1AA").alias("postcode"),
        F.lit("0").alias("driving_points"),
        F.lit("5").alias("driving_years"),
        F.lit("BADRC000000AB00").alias("licence_number"),
        F.lit("2024-01-01").alias("created_date"),
        F.lit("London").alias("city"),
    )
)

to_str(ph_df).unionByName(to_str(bad_ph)).write.mode("overwrite").option("header", "true").csv(f"{LANDING}/policyholders")
print(f"✓ policyholders   : {N_PH:,} clean + {N_BAD_PH:,} bad = {N_PH + N_BAD_PH:,} total")

# COMMAND ----------
# ── 2. VEHICLES ──────────────────────────────────────────────────────────────

# UK-popular makes (VW and Ford top sellers, Vauxhall, Toyota widely driven)
UK_MAKES  = ["Volkswagen", "Ford", "Vauxhall", "Toyota", "BMW", "Nissan", "Audi", "Mercedes-Benz", "Hyundai", "Land Rover"]
make_arr  = F.array([F.lit(m) for m in UK_MAKES])
VTYPES    = ["Hatchback", "Saloon", "SUV", "Estate", "MPV", "Crossover", "Coupe"]
vtype_arr = F.array([F.lit(v) for v in VTYPES])
COLORS    = ["White", "Black", "Silver", "Grey", "Blue", "Red", "Green", "Bronze", "Orange"]
color_arr = F.array([F.lit(c) for c in COLORS])

vh_df = (
    spark.range(0, N_VH, numPartitions=8)
    .select(
        F.concat(F.lit("VH-"), F.lpad((F.col("id") + 1).cast("string"), 6, "0")).alias("vehicle_id"),
        F.concat(F.lit("PH-"), F.lpad(((F.abs(F.hash(F.col("id"))) % N_PH) + 1).cast("string"), 6, "0")).alias("policyholder_id"),
        F.element_at(make_arr,  (F.abs(F.hash(F.col("id")))     % len(UK_MAKES) + 1).cast(IntegerType())).alias("make"),
        F.element_at(vtype_arr, (F.abs(F.hash(F.col("id") + 1)) % len(VTYPES)  + 1).cast(IntegerType())).alias("vehicle_type"),
        (F.abs(F.hash(F.col("id") + 2)) % 15 + 2010).cast(IntegerType()).alias("year"),
        udf_vin(F.col("id")).alias("vin"),
        F.element_at(color_arr, (F.abs(F.hash(F.col("id") + 3)) % len(COLORS) + 1).cast(IntegerType())).alias("colour"),
        F.round(F.exp(F.randn(10) * 0.5 + 9.7), 2).alias("estimated_value"),
        (F.abs(F.hash(F.col("id") + 4)) % 150000 + 5000).cast(IntegerType()).alias("mileage"),
        F.lit("2024-01-01").alias("created_date"),
    )
)

bad_vh = (
    spark.range(N_VH, N_VH + N_BAD_VH, numPartitions=2)
    .select(
        F.when(F.col("id") % 3 == 0, F.lit(None))
         .otherwise(F.concat(F.lit("BAD-"), F.lpad((F.col("id")+1).cast("string"), 6, "0"))).alias("vehicle_id"),
        F.concat(F.lit("PH-"), F.lpad(((F.abs(F.hash(F.col("id"))) % N_PH) + 1).cast("string"), 6, "0")).alias("policyholder_id"),
        F.lit("Ford").alias("make"),
        F.lit("Hatchback").alias("vehicle_type"),
        F.when(F.col("id") % 3 == 1, F.lit("1850")).otherwise(F.lit("2020")).alias("year"),
        F.lit("BADVINTESTONLY0AB").alias("vin"),
        F.lit("White").alias("colour"),
        F.when(F.col("id") % 3 == 2, F.lit("-999.0")).otherwise(F.lit("15000.0")).alias("estimated_value"),
        F.lit("45000").alias("mileage"),
        F.lit("2024-01-01").alias("created_date"),
    )
)

to_str(vh_df).unionByName(to_str(bad_vh)).write.mode("overwrite").option("header", "true").csv(f"{LANDING}/vehicles")
print(f"✓ vehicles         : {N_VH:,} clean + {N_BAD_VH:,} bad = {N_VH + N_BAD_VH:,} total")

# COMMAND ----------
# ── 3. POLICIES ──────────────────────────────────────────────────────────────

POL_TYPES = ["Comprehensive", "ThirdPartyFireTheft", "ThirdPartyOnly"]
COV_TYPES = ["Accidental Damage", "Fire and Theft", "Third Party Liability",
             "Windscreen Cover", "Courtesy Car"]
STATUSES  = ["Active", "Active", "Active", "Active", "Active", "Active", "Lapsed", "Cancelled"]

pol_df = (
    spark.range(0, N_POL, numPartitions=8)
    .select(
        F.concat(F.lit("POL-"), F.lpad((F.col("id") + 1).cast("string"), 6, "0")).alias("policy_id"),
        F.concat(F.lit("PH-"), F.lpad(((F.abs(F.hash(F.col("id")))      % N_PH) + 1).cast("string"), 6, "0")).alias("policyholder_id"),
        F.concat(F.lit("VH-"), F.lpad(((F.abs(F.hash(F.col("id") + 50)) % N_VH) + 1).cast("string"), 6, "0")).alias("vehicle_id"),
        F.element_at(F.array([F.lit(p) for p in POL_TYPES]), (F.abs(F.hash(F.col("id") + 1)) % len(POL_TYPES) + 1).cast(IntegerType())).alias("policy_type"),
        F.element_at(F.array([F.lit(c) for c in COV_TYPES]), (F.abs(F.hash(F.col("id") + 2)) % len(COV_TYPES) + 1).cast(IntegerType())).alias("coverage_type"),
        F.round(F.exp(F.randn(11) * 0.3 + 5.1), 2).alias("premium_monthly"),
        F.element_at(F.array(F.lit(250.0), F.lit(500.0), F.lit(1000.0), F.lit(2000.0)),
            (F.abs(F.hash(F.col("id") + 3)) % 4 + 1).cast(IntegerType())).alias("excess"),
        F.round(F.exp(F.randn(12) * 0.4 + 11.3), 2).alias("coverage_limit"),
        F.date_sub(F.lit("2026-02-17").cast(DateType()), (F.abs(F.hash(F.col("id") + 4)) % 730).cast(IntegerType())).alias("start_date"),
        F.date_add(F.date_sub(F.lit("2026-02-17").cast(DateType()), (F.abs(F.hash(F.col("id") + 4)) % 730).cast(IntegerType())), 365).alias("end_date"),
        F.element_at(F.array([F.lit(s) for s in STATUSES]), (F.abs(F.hash(F.col("id") + 5)) % len(STATUSES) + 1).cast(IntegerType())).alias("status"),
        F.lit("2024-01-01").alias("created_date"),
    )
)

bad_pol = (
    spark.range(N_POL, N_POL + N_BAD_POL, numPartitions=2)
    .select(
        F.when(F.col("id") % 3 == 0, F.lit(None))
         .otherwise(F.concat(F.lit("BAD-"), F.lpad((F.col("id")+1).cast("string"), 6, "0"))).alias("policy_id"),
        F.concat(F.lit("PH-"), F.lpad(((F.abs(F.hash(F.col("id")))      % N_PH) + 1).cast("string"), 6, "0")).alias("policyholder_id"),
        F.concat(F.lit("VH-"), F.lpad(((F.abs(F.hash(F.col("id") + 50)) % N_VH) + 1).cast("string"), 6, "0")).alias("vehicle_id"),
        F.lit("Comprehensive").alias("policy_type"),
        F.lit("Accidental Damage").alias("coverage_type"),
        F.when(F.col("id") % 3 == 2, F.lit("-200.0")).otherwise(F.lit("120.0")).alias("premium_monthly"),
        F.lit("500.0").alias("excess"),
        F.lit("50000.0").alias("coverage_limit"),
        F.lit("2025-06-01").alias("start_date"),
        F.when(F.col("id") % 3 == 1, F.lit("2023-06-01")).otherwise(F.lit("2026-06-01")).alias("end_date"),
        F.lit("Active").alias("status"),
        F.lit("2024-01-01").alias("created_date"),
    )
)

to_str(pol_df).unionByName(to_str(bad_pol)).write.mode("overwrite").option("header", "true").csv(f"{LANDING}/policies")
print(f"✓ policies         : {N_POL:,} clean + {N_BAD_POL:,} bad = {N_POL + N_BAD_POL:,} total")

# COMMAND ----------
# ── 4. CLAIMS ────────────────────────────────────────────────────────────────

CLM_STATUSES = ["open", "under_review", "approved", "denied", "settled"]

# Storm Freya: concentrated in flood-prone UK regions
STORM_REGIONS = (
    ["West Yorkshire"]     * 30 +   # severe flooding
    ["South Yorkshire"]    * 25 +   # Doncaster/Sheffield flooding
    ["Somerset"]           * 20 +   # Somerset Levels flooding
    ["Greater Manchester"] * 15 +
    ["Devon"]              * 10
)
STORM_REG_ARR = F.array([F.lit(r) for r in STORM_REGIONS])

# Non-storm regions spread across UK
NON_STORM_REGIONS = (
    ["Greater London"]     * 25 +
    ["West Midlands"]      * 12 +
    ["Merseyside"]         * 8  +
    ["Hampshire"]          * 8  +
    ["Essex"]              * 7  +
    ["Kent"]               * 7  +
    ["Lancashire"]         * 6  +
    ["Nottinghamshire"]    * 5  +
    ["Tyne and Wear"]      * 5  +
    ["Bristol"]            * 5  +
    ["North Yorkshire"]    * 5  +
    ["West Yorkshire"]     * 7
)
NON_STORM_ARR = F.array([F.lit(r) for r in NON_STORM_REGIONS])

clm_df = (
    spark.range(0, N_CLM, numPartitions=16)
    .withColumn("is_storm", F.rand(20) < STORM_RATIO)
    .select(
        F.col("id"), F.col("is_storm"),
        F.concat(F.lit("CLM-"), F.lpad((F.col("id") + 1).cast("string"), 6, "0")).alias("claim_id"),
        F.concat(F.lit("POL-"), F.lpad(((F.abs(F.hash(F.col("id")))      % N_POL) + 1).cast("string"), 6, "0")).alias("policy_id"),
        F.concat(F.lit("PH-"),  F.lpad(((F.abs(F.hash(F.col("id") + 10)) % N_PH)  + 1).cast("string"), 6, "0")).alias("policyholder_id"),
        F.concat(F.lit("VH-"),  F.lpad(((F.abs(F.hash(F.col("id") + 20)) % N_VH)  + 1).cast("string"), 6, "0")).alias("vehicle_id"),
        F.when(F.col("is_storm"), F.lit("weather"))
         .when(F.rand(21) < 0.45, F.lit("collision")).when(F.rand(22) < 0.65, F.lit("theft"))
         .when(F.rand(23) < 0.85, F.lit("vandalism")).otherwise(F.lit("fire")).alias("claim_type"),
        F.to_date(F.from_unixtime(
            F.when(F.col("is_storm"), (F.rand(24) * (STORM_END_DAYS - STORM_START_DAYS) + STORM_START_DAYS) * 86400)
            .otherwise((F.rand(25) * (HIST_END_DAYS - HIST_START_DAYS) + HIST_START_DAYS) * 86400)
        )).alias("claim_date"),
        # Storm regions: Yorkshire/Somerset; non-storm: spread across UK
        F.when(F.col("is_storm"),
            F.element_at(STORM_REG_ARR, (F.abs(F.hash(F.col("id") + 30)) % len(STORM_REGIONS) + 1).cast(IntegerType())))
        .otherwise(
            F.element_at(NON_STORM_ARR, (F.abs(F.hash(F.col("id") + 31)) % len(NON_STORM_REGIONS) + 1).cast(IntegerType()))
        ).alias("incident_region"),
        # Higher severity during storm
        F.when(F.col("is_storm"),
            F.when(F.rand(40) < 0.12, F.lit("total_loss")).when(F.rand(41) < 0.42, F.lit("severe"))
             .when(F.rand(42) < 0.80, F.lit("moderate")).otherwise(F.lit("minor")))
        .otherwise(
            F.when(F.rand(43) < 0.05, F.lit("total_loss")).when(F.rand(44) < 0.20, F.lit("severe"))
             .when(F.rand(45) < 0.55, F.lit("moderate")).otherwise(F.lit("minor"))).alias("severity_label"),
        # 14% fraud rate during storm, 8% baseline
        F.when(F.col("is_storm"),
            F.round(F.when(F.rand(50) < 0.14, F.rand(51) * 0.4 + 0.6).otherwise(F.rand(52) * 0.45), 4))
        .otherwise(
            F.round(F.when(F.rand(53) < 0.08, F.rand(54) * 0.4 + 0.6).otherwise(F.rand(55) * 0.45), 4)).alias("fraud_risk_score"),
        # Claim amounts in GBP
        F.round(F.exp(
            F.when(F.col("is_storm"), F.randn(60) * 0.6 + 8.4)
            .otherwise(F.randn(61) * 0.7 + 7.7)
        ), 2).alias("claim_amount_gbp"),
        F.element_at(F.array([F.lit(s) for s in CLM_STATUSES]),
            (F.abs(F.hash(F.col("id") + 70)) % len(CLM_STATUSES) + 1).cast(IntegerType())).alias("claim_status"),
        F.when(F.col("is_storm"), F.lit("Y")).otherwise(F.lit("N")).alias("is_storm_related"),
        F.lit("2026-02-17").alias("created_date"),
    )
    .withColumn("incident_city", udf_city_for_region(F.col("incident_region")))
    .withColumn("description", udf_claim_desc(F.col("claim_type")))
    .drop("id", "is_storm")
)

bad_clm = (
    spark.range(N_CLM, N_CLM + N_BAD_CLM, numPartitions=4)
    .select(
        F.when(F.col("id") % 4 == 0, F.lit(None))
         .otherwise(F.concat(F.lit("BAD-"), F.lpad((F.col("id")+1).cast("string"), 6, "0"))).alias("claim_id"),
        F.concat(F.lit("POL-"), F.lpad(((F.abs(F.hash(F.col("id")))      % N_POL) + 1).cast("string"), 6, "0")).alias("policy_id"),
        F.concat(F.lit("PH-"),  F.lpad(((F.abs(F.hash(F.col("id") + 10)) % N_PH)  + 1).cast("string"), 6, "0")).alias("policyholder_id"),
        F.concat(F.lit("VH-"),  F.lpad(((F.abs(F.hash(F.col("id") + 20)) % N_VH)  + 1).cast("string"), 6, "0")).alias("vehicle_id"),
        F.lit("collision").alias("claim_type"),
        F.lit("2025-09-15").alias("claim_date"),
        F.lit("Greater London").alias("incident_region"),
        F.when(F.col("id") % 4 == 2, F.lit("CATASTROPHIC")).otherwise(F.lit("moderate")).alias("severity_label"),
        F.when(F.col("id") % 4 == 3, F.lit("2.5")).otherwise(F.lit("0.3")).alias("fraud_risk_score"),
        F.when(F.col("id") % 4 == 1, F.lit("-5000.0")).otherwise(F.lit("7500.0")).alias("claim_amount_gbp"),
        F.lit("open").alias("claim_status"),
        F.lit("N").alias("is_storm_related"),
        F.lit("2026-02-17").alias("created_date"),
        F.lit("London").alias("incident_city"),
        F.lit("Bad data test record").alias("description"),
    )
)

to_str(clm_df).unionByName(to_str(bad_clm)).write.mode("overwrite").option("header", "true").csv(f"{LANDING}/claims")
print(f"✓ claims           : {N_CLM:,} clean + {N_BAD_CLM:,} bad = {N_CLM + N_BAD_CLM:,} total")

# COMMAND ----------
# ── 5. INCIDENTS ─────────────────────────────────────────────────────────────

INC_TYPES  = ["collision", "flood_damage", "storm_damage", "theft", "vandalism", "fire"]
WEATHER    = ["clear", "heavy_rain", "flood", "snow", "fog", "strong_wind", "hail"]
ROAD       = ["dry", "wet", "flooded", "icy", "waterlogged", "debris_on_road"]
VISIBILITY = ["good", "moderate", "poor"]

inc_df = (
    spark.range(0, N_INC, numPartitions=16)
    .select(
        F.concat(F.lit("INC-"), F.lpad((F.col("id") + 1).cast("string"), 6, "0")).alias("incident_id"),
        F.concat(F.lit("CLM-"), F.lpad(((F.abs(F.hash(F.col("id"))) % N_CLM) + 1).cast("string"), 6, "0")).alias("claim_id"),
        F.element_at(F.array([F.lit(t) for t in INC_TYPES]),  (F.abs(F.hash(F.col("id") + 1)) % len(INC_TYPES)  + 1).cast(IntegerType())).alias("incident_type"),
        F.to_date(F.from_unixtime((F.rand(70) * (STORM_END_DAYS - HIST_START_DAYS) + HIST_START_DAYS) * 86400)).alias("incident_date"),
        F.element_at(F.array([F.lit(w) for w in WEATHER]),    (F.abs(F.hash(F.col("id") + 2)) % len(WEATHER)    + 1).cast(IntegerType())).alias("weather_condition"),
        F.element_at(F.array([F.lit(r) for r in ROAD]),       (F.abs(F.hash(F.col("id") + 3)) % len(ROAD)       + 1).cast(IntegerType())).alias("road_condition"),
        F.element_at(F.array([F.lit(v) for v in VISIBILITY]), (F.abs(F.hash(F.col("id") + 4)) % len(VISIBILITY)  + 1).cast(IntegerType())).alias("visibility"),
        F.when(F.rand(71) < 0.70, F.lit("Y")).otherwise(F.lit("N")).alias("police_report_filed"),
        (F.abs(F.hash(F.col("id") + 5)) % 4 + 1).cast(IntegerType()).alias("num_vehicles_involved"),
        (F.abs(F.hash(F.col("id") + 6)) % 5).cast(IntegerType()).alias("witness_count"),
        F.lit("2026-02-17").alias("created_date"),
    )
)

bad_inc = (
    spark.range(N_INC, N_INC + N_BAD_INC, numPartitions=2)
    .select(
        F.when(F.col("id") % 2 == 0, F.lit(None))
         .otherwise(F.concat(F.lit("BAD-"), F.lpad((F.col("id")+1).cast("string"), 6, "0"))).alias("incident_id"),
        F.when(F.col("id") % 2 == 1, F.lit(None))
         .otherwise(F.concat(F.lit("CLM-"), F.lpad(((F.abs(F.hash(F.col("id"))) % N_CLM) + 1).cast("string"), 6, "0"))).alias("claim_id"),
        F.lit("collision").alias("incident_type"),
        F.lit("2025-09-15").alias("incident_date"),
        F.lit("clear").alias("weather_condition"),
        F.lit("dry").alias("road_condition"),
        F.lit("good").alias("visibility"),
        F.lit("N").alias("police_report_filed"),
        F.lit("1").alias("num_vehicles_involved"),
        F.lit("0").alias("witness_count"),
        F.lit("2026-02-17").alias("created_date"),
    )
)

to_str(inc_df).unionByName(to_str(bad_inc)).write.mode("overwrite").option("header", "true").csv(f"{LANDING}/incidents")
print(f"✓ incidents        : {N_INC:,} clean + {N_BAD_INC:,} bad = {N_INC + N_BAD_INC:,} total")

# COMMAND ----------
# ── 6. CLAIM ASSESSMENTS ─────────────────────────────────────────────────────

DMG_CATS    = ["exterior_bodywork", "mechanical", "total_loss", "interior", "glass_windscreen", "electrical", "flood_damage"]
REC_ACTS    = ["repair", "replace", "write_off", "deny", "further_investigation"]
N_ASSESSORS = 200

ass_df = (
    spark.range(0, N_ASS, numPartitions=16)
    .select(
        F.concat(F.lit("ASS-"), F.lpad((F.col("id") + 1).cast("string"), 6, "0")).alias("assessment_id"),
        F.concat(F.lit("CLM-"), F.lpad(((F.abs(F.hash(F.col("id"))) % N_CLM) + 1).cast("string"), 6, "0")).alias("claim_id"),
        F.concat(F.lit("ASR-"), F.lpad(((F.abs(F.hash(F.col("id") + 100)) % N_ASSESSORS) + 1).cast("string"), 4, "0")).alias("assessor_id"),
        F.when(F.rand(80) < 0.45, F.lit("junior")).when(F.rand(81) < 0.80, F.lit("senior"))
         .otherwise(F.lit("specialist")).alias("assessor_tier"),
        F.to_date(F.from_unixtime((F.rand(82) * (STORM_END_DAYS - HIST_START_DAYS) + HIST_START_DAYS) * 86400)).alias("assessment_date"),
        F.element_at(F.array([F.lit(d) for d in DMG_CATS]), (F.abs(F.hash(F.col("id") + 2)) % len(DMG_CATS) + 1).cast(IntegerType())).alias("damage_category"),
        F.round(F.exp(F.randn(83) * 0.6 + 7.4), 2).alias("estimated_repair_cost_gbp"),
        F.round(F.rand(84) * 40 + 2, 1).alias("labour_hours"),
        F.round(F.exp(F.randn(85) * 0.5 + 6.4), 2).alias("parts_cost_gbp"),
        F.element_at(F.array([F.lit(a) for a in REC_ACTS]), (F.abs(F.hash(F.col("id") + 3)) % len(REC_ACTS) + 1).cast(IntegerType())).alias("recommended_action"),
        F.lit("2026-02-17").alias("created_date"),
    )
)

bad_ass = (
    spark.range(N_ASS, N_ASS + N_BAD_ASS, numPartitions=4)
    .select(
        F.when(F.col("id") % 3 == 0, F.lit(None))
         .otherwise(F.concat(F.lit("BAD-"), F.lpad((F.col("id")+1).cast("string"), 6, "0"))).alias("assessment_id"),
        F.concat(F.lit("CLM-"), F.lpad(((F.abs(F.hash(F.col("id"))) % N_CLM) + 1).cast("string"), 6, "0")).alias("claim_id"),
        F.lit("ASR-0001").alias("assessor_id"),
        F.lit("junior").alias("assessor_tier"),
        F.lit("2025-09-15").alias("assessment_date"),
        F.lit("exterior_bodywork").alias("damage_category"),
        F.when(F.col("id") % 3 == 1, F.lit("-3000.0")).otherwise(F.lit("4500.0")).alias("estimated_repair_cost_gbp"),
        F.when(F.col("id") % 3 == 2, F.lit("-5.0")).otherwise(F.lit("10.0")).alias("labour_hours"),
        F.lit("1500.0").alias("parts_cost_gbp"),
        F.lit("repair").alias("recommended_action"),
        F.lit("2026-02-17").alias("created_date"),
    )
)

to_str(ass_df).unionByName(to_str(bad_ass)).write.mode("overwrite").option("header", "true").csv(f"{LANDING}/claim_assessments")
print(f"✓ claim_assessments: {N_ASS:,} clean + {N_BAD_ASS:,} bad = {N_ASS + N_BAD_ASS:,} total")

# COMMAND ----------
# MAGIC %md ### Summary

print("\n── Landing Volume: row counts ────────────────────────────────────")
totals = {
    "policyholders":    (N_PH,  N_BAD_PH),
    "vehicles":         (N_VH,  N_BAD_VH),
    "policies":         (N_POL, N_BAD_POL),
    "claims":           (N_CLM, N_BAD_CLM),
    "incidents":        (N_INC, N_BAD_INC),
    "claim_assessments":(N_ASS, N_BAD_ASS),
}
total_clean = total_bad = 0
for t, (clean, bad) in totals.items():
    print(f"  {t:<22}  {clean:>7,} clean  {bad:>5,} bad ({bad/(clean+bad)*100:.1f}%)")
    total_clean += clean
    total_bad   += bad
print(f"  {'TOTAL':<22}  {total_clean:>7,} clean  {total_bad:>5,} bad ({total_bad/(total_clean+total_bad)*100:.1f}%)")
print("\nStorm Freya (Feb 3–17 2026): ~35% of claims concentrated in Yorkshire, Somerset, Manchester")
print("Next step: run Landing to Bronze → Bronze to Silver to see DQ rules in action")

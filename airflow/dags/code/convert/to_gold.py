# -*- coding: utf-8 -*-
"""
to_gold.py

Gold layer (đầy đủ) tạo các bảng phục vụ dashboard từ SILVER.
Tối ưu/điều chỉnh tối thiểu theo trao đổi:
- Đọc SILVER trực tiếp trên S3 (đọc toàn bộ partitions) để tạo "overall" + các bảng theo ngày.
- Chuẩn hoá: 1 chuyến = 1 dòng (khử trùng lặp theo khóa tự nhiên), bucketing giờ, clamp giá/vé.
- Join luôn bằng bus_name_norm để nhất quán.
- Câu 1 giữ nguyên "OVERALL" (không theo ngày) như bạn yêu cầu.
- Ghi bảng GOLD vào Iceberg Catalog "lakehouse".

YÊU CẦU: đã có spark_config.get_spark_session() cấu hình Iceberg + S3.
"""

import os
import re
from pyspark.sql import functions as F, types as T
from dags.code.spark_session.spark_config import get_spark_session
from pyspark.sql import Window


# =========================
# Env (tối thiểu)
# =========================
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")  # đọc toàn bộ partitions trong bucket silver
CATALOG_PREFIX = "lakehouse.gold"                     # nơi ghi các bảng GOLD


# =========================
# Helpers
# =========================
def _hour_bucket_py(h: int) -> str:
    if h is None:
        return None
    if 0 <= h <= 5:
        return "00-05"
    if 6 <= h <= 11:
        return "06-11"
    if 12 <= h <= 17:
        return "12-17"
    return "18-23"

hour_bucket = F.udf(_hour_bucket_py, T.StringType())


def _ensure_table(spark, full_name: str, schema_sql: str, partition_by: str | None = None):
    """
    Tạo bảng Iceberg nếu chưa có, với schema cố định.
    """
    part_sql = "" if not partition_by else f" PARTITIONED BY ({partition_by})"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_name} (
            {schema_sql}
        )
        USING iceberg{part_sql}
    """)


# =========================
# Load SILVER (đọc toàn bộ partitions)
# =========================
def load_silver_ticket(spark):
    # path parquet: s3a://silver/ticket/date=YYYY-MM-DD/silver_ticket_YYYY-MM-DD.parquet
    path = f"s3a://{SILVER_BUCKET}/ticket/"
    df = spark.read.parquet(path)

    # Bảo toàn các cột cần thiết
    need = {
        "bus_name_norm", "Route", "date",
        "departure_ts", "arrival_ts",
        "price_vnd", "Ticket_Sold",
        "Seat_Types", "Seat_Available"
    }
    cols = [c for c in df.columns if c in need]
    return df.select(*cols)


def load_silver_facility(spark):
    # s3a://silver/facility/date=YYYY-MM-DD/silver_facility_YYYY-MM-DD.parquet
    path = f"s3a://{SILVER_BUCKET}/facility/"
    df = spark.read.parquet(path)
    need = {"bus_name_norm", "Facility", "event_date"}
    return df.select(*[c for c in df.columns if c in need])


def load_silver_review_scored(spark):
    # s3a://silver/review/date=YYYY-MM-DD/silver_reviews_YYYY-MM-DD.parquet
    # (đã join sentiment ở to_silver)
    path = f"s3a://{SILVER_BUCKET}/review/"
    df = spark.read.parquet(path)

    # BỔ SUNG: nếu thiếu bus_name_norm thì suy ra từ Bus_Name (đồng bộ với ticket)
    if "bus_name_norm" not in df.columns and "Bus_Name" in df.columns:
        def _bus_norm_py(s: str) -> str:
            if s is None:
                return ""
            s = s.strip().lower()
            s = re.sub(r"\s+", " ", s)
            s = re.sub(r"[^a-z0-9\s\-]", "", s)
            return s
        bus_norm_udf = F.udf(_bus_norm_py, T.StringType())
        df = df.withColumn("bus_name_norm", bus_norm_udf(F.col("Bus_Name")))

    # các cột phổ biến sau to_silver
    keep = {
        "Bus_Name", "bus_name_norm", "comment", "lang", "event_date",
        "NEG", "NEU", "POS", "sentiment_score", "sentiment_label"
    }
    cols = [c for c in df.columns if c in keep]
    return df.select(*cols)


# =========================
# Chuẩn hoá Ticket cho GOLD
# =========================
def ticket_clean_enriched(df_ticket):
    """
    - 1 chuyến = 1 dòng: dropDuplicates theo (bus_name_norm, Route, departure_ts)
    - ép kiểu, clamp giá/vé
    - thêm bucket giờ
    """
    out = (
        df_ticket
        .withColumn("departure_ts", F.col("departure_ts").cast("timestamp"))
        .withColumn("arrival_ts",   F.col("arrival_ts").cast("timestamp"))
        .withColumn("price_vnd",    F.col("price_vnd").cast("int"))
        .withColumn("Ticket_Sold",  F.col("Ticket_Sold").cast("int"))
        .dropna(subset=["bus_name_norm", "Route", "departure_ts"])
        .dropDuplicates(["bus_name_norm", "Route", "departure_ts"])
        .withColumn("Ticket_Sold", F.coalesce(F.col("Ticket_Sold"), F.lit(1)))  # fallback an toàn
        .withColumn("price_vnd",
            F.when(F.col("price_vnd") <= 0, None).otherwise(F.col("price_vnd"))
        )
        .withColumn("Ticket_Sold",
            F.when(F.col("Ticket_Sold") < 0, F.lit(0)).otherwise(F.col("Ticket_Sold"))
        )
        .withColumn("dep_hour",   F.hour("departure_ts"))
        .withColumn("dep_bucket", hour_bucket(F.col("dep_hour")))
        .withColumn("start_date",
            F.coalesce(F.col("date").cast("date"), F.to_date("departure_ts"))
        )
    )
    return out


# =========================
# GOLD: CÂU 1 (OVERALL, giữ nguyên)
# =========================
def build_cau_1_overall(spark, ticket_enriched):
    gold_tbl = f"{CATALOG_PREFIX}.cau_1_overall"
    _ensure_table(
        spark,
        gold_tbl,
        schema_sql="""
            route STRING,
            bus_name_norm STRING,
            dep_bucket STRING,
            sum_tickets_sold BIGINT,
            avg_price_vnd DOUBLE
        """,
        partition_by=None
    )

    cau_1 = (
        ticket_enriched
        .groupBy("Route", "bus_name_norm", "dep_bucket")
        .agg(
            F.sum("Ticket_Sold").alias("sum_tickets_sold"),
            F.avg("price_vnd").alias("avg_price_vnd"),
        )
        .withColumnRenamed("Route", "route")
    )

    (
        cau_1
        .select("route", "bus_name_norm", "dep_bucket",
                "sum_tickets_sold", "avg_price_vnd")
        .repartition(1)
        .writeTo(gold_tbl)
        .overwritePartitions()  # bảng overall không partition -> overwrite toàn bảng
    )


# =========================
# GOLD: CÂU 2 (min price theo ngày/route + sentiment dương trung bình)
# =========================
def build_cau_2_min_price_daily(spark, ticket_enriched, review_scored):
    gold_tbl = f"{CATALOG_PREFIX}.cau_2_min_price_daily"
    _ensure_table(
        spark,
        gold_tbl,
        schema_sql="""
            start_date DATE,
            route STRING,
            min_price_vnd INT,
            avg_pos DOUBLE
        """,
        partition_by="start_date"
    )

    daily_min = (
        ticket_enriched
        .groupBy("start_date", "Route")
        .agg(F.min("price_vnd").alias("min_price_vnd"))
        .withColumnRenamed("Route", "route")
    )

    # sentiment dương theo ngày/route (map theo bus)
    pos_by_day_route = (
        review_scored
        .join(
            ticket_enriched.select("bus_name_norm", "Route", "start_date").dropDuplicates(),
            on=["bus_name_norm"], how="left"
        )
        .groupBy("start_date", "Route")
        .agg(F.avg("POS").alias("avg_pos"))
        .withColumnRenamed("Route", "route")
    )

    out = (
        daily_min.alias("a")
        .join(pos_by_day_route.alias("b"), on=["start_date", "route"], how="left")
        .select("start_date", "route", "min_price_vnd", "avg_pos")
    )

    (
        out.repartition("start_date")
        .writeTo(gold_tbl)
        .overwritePartitions()
    )


# =========================
# GOLD: CÂU 3 (top route theo tổng chuyến/ngày)
# =========================
def build_cau_3_top_routes_daily(spark, ticket_enriched):
    gold_tbl = f"{CATALOG_PREFIX}.cau_3_top_routes_daily"
    _ensure_table(
        spark,
        gold_tbl,
        schema_sql="""
            start_date DATE,
            route STRING,
            trips BIGINT
        """,
        partition_by="start_date"
    )

    out = (
        ticket_enriched
        .groupBy("start_date", "Route")
        .agg(F.sum("Ticket_Sold").alias("trips"))
        .withColumnRenamed("Route", "route")
    )

    (
        out.repartition("start_date")
        .writeTo(gold_tbl)
        .overwritePartitions()
    )


# =========================
# GOLD: CÂU 4 (avg price theo ngày/route)
# =========================
def build_cau_4_avg_price_by_date_route(spark, ticket_enriched):
    gold_tbl = f"{CATALOG_PREFIX}.cau_4_avg_price_by_date_route"
    _ensure_table(
        spark,
        gold_tbl,
        schema_sql="""
            start_date DATE,
            route STRING,
            avg_price_vnd DOUBLE
        """,
        partition_by="start_date"
    )

    out = (
        ticket_enriched
        .groupBy("start_date", "Route")
        .agg(F.avg("price_vnd").alias("avg_price_vnd"))
        .withColumnRenamed("Route", "route")
    )

    (
        out.repartition("start_date")
        .writeTo(gold_tbl)
        .overwritePartitions()
    )


# =========================
# GOLD: CÂU 5 (đếm review & sentiment theo bus)
# =========================
def build_cau_5_review_counts(spark, review_scored):
    gold_tbl = f"{CATALOG_PREFIX}.cau_5_review_counts"
    _ensure_table(
        spark,
        gold_tbl,
        schema_sql="""
            bus_name_norm STRING,
            review_cnt BIGINT,
            avg_pos DOUBLE,
            avg_neg DOUBLE,
            avg_neu DOUBLE
        """,
        partition_by=None
    )

    out = (
        review_scored
        .groupBy("bus_name_norm")
        .agg(
            F.count("*").alias("review_cnt"),
            F.avg("POS").alias("avg_pos"),
            F.avg("NEG").alias("avg_neg"),
            F.avg("NEU").alias("avg_neu"),
        )
    )

    (
        out.repartition(1)
        .writeTo(gold_tbl)
        .overwritePartitions()
    )


# =========================
# GOLD: CÂU 6 (xếp hạng bus theo sentiment, lọc ngưỡng review)
# =========================
def build_cau_6_ranked_buses_by_sentiment(spark, review_scored, min_reviews: int = 50):
    gold_tbl = f"{CATALOG_PREFIX}.cau_6_ranked_buses_by_sentiment"
    _ensure_table(
        spark,
        gold_tbl,
        schema_sql="""
            bus_name_norm STRING,
            review_cnt BIGINT,
            avg_pos DOUBLE,
            rank_pos INT
        """,
        partition_by=None
    )

    base = (
        review_scored
        .groupBy("bus_name_norm")
        .agg(
            F.count("*").alias("review_cnt"),
            F.avg("POS").alias("avg_pos"),
        )
        .filter(F.col("review_cnt") >= F.lit(min_reviews))
    )

    # win = F.window  # not used; just to avoid confusion
    out = base.withColumn(
        "rank_pos",
        F.dense_rank().over(
            Window.orderBy(F.col("avg_pos").desc())
        )
    )

    (
        out.repartition(1)
        .writeTo(gold_tbl)
        .overwritePartitions()
    )


# =========================
# GOLD: CÂU 7 (ma trận giờ 0-23 x Bus_Name: có chuyến?)
# =========================
def build_cau_7_hour_bus_matrix(spark, ticket_enriched):
    gold_tbl = f"{CATALOG_PREFIX}.cau_7_hour_bus_matrix"
    _ensure_table(
        spark,
        gold_tbl,
        schema_sql="""
            bus_name_norm STRING,
            hour INT,
            has_trip INT
        """,
        partition_by=None
    )

    by_hour = (
        ticket_enriched
        .select("bus_name_norm", F.hour("departure_ts").alias("hour"))
        .groupBy("bus_name_norm", "hour")
        .agg(F.count("*").alias("cnt"))
        .withColumn("has_trip", F.when(F.col("cnt") > 0, F.lit(1)).otherwise(F.lit(0)))
        .select("bus_name_norm", "hour", "has_trip")
    )

    (
        by_hour.repartition(1)
        .writeTo(gold_tbl)
        .overwritePartitions()
    )


# =========================
# GOLD: CÂU 8 (dimension Facility + mapping bus-facility)
# =========================
def build_cau_8_facility_maps(spark, facility_df):
    dim_tbl = f"{CATALOG_PREFIX}.dim_facility"
    map_tbl = f"{CATALOG_PREFIX}.bus_facility"

    _ensure_table(
        spark,
        dim_tbl,
        schema_sql="""
            facility_id INT,
            facility STRING
        """,
        partition_by=None
    )
    _ensure_table(
        spark,
        map_tbl,
        schema_sql="""
            bus_name_norm STRING,
            facility_id INT
        """,
        partition_by=None
    )

    # Dimension facility (id ổn định theo thứ tự chữ cái)
    facilities = (
        facility_df
        .select(F.lower(F.col("Facility")).alias("facility"))
        .dropna()
        .dropDuplicates(["facility"])
        .withColumn("facility_id", F.dense_rank().over(
            Window.orderBy(F.col("facility"))
        ))
        .select("facility_id", "facility")
    )

    (
        facilities.repartition(1)
        .writeTo(dim_tbl)
        .overwritePartitions()
    )

    # Mapping bus <-> facility
    bus_fac = (
        facility_df
        .select("bus_name_norm", F.lower(F.col("Facility")).alias("facility"))
        .dropna()
        .dropDuplicates()
        .join(facilities, on="facility", how="inner")
        .select("bus_name_norm", "facility_id")
    )

    (
        bus_fac.repartition(1)
        .writeTo(map_tbl)
        .overwritePartitions()
    )


# =========================
# MAIN
# =========================
def main():
    spark = get_spark_session("to_gold_full")

    # Load SILVER từ S3 (đã chuẩn hoá ở to_silver)
    df_ticket_raw   = load_silver_ticket(spark)
    df_facility_raw = load_silver_facility(spark)
    df_review_scored = load_silver_review_scored(spark)

    # Chuẩn hoá ticket dùng chung
    tix = ticket_clean_enriched(df_ticket_raw)

    # --- CÂU 1: Overall (không theo ngày) ---
    build_cau_1_overall(spark, tix)

    # --- Các bảng GOLD bổ sung (phục vụ dashboard/time series) ---
    build_cau_2_min_price_daily(spark, tix, df_review_scored)
    build_cau_3_top_routes_daily(spark, tix)
    build_cau_4_avg_price_by_date_route(spark, tix)
    build_cau_5_review_counts(spark, df_review_scored)
    build_cau_6_ranked_buses_by_sentiment(spark, df_review_scored, min_reviews=50)
    build_cau_7_hour_bus_matrix(spark, tix)
    build_cau_8_facility_maps(spark, df_facility_raw)

    print("✅ GOLD layer created/updated successfully.")

def to_gold():
    return main()

if __name__ == "__main__":
    main()
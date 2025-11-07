# -*- coding: utf-8 -*-
"""
to_silver.py
- Đọc BRONZE từ MinIO (s3a)
- Làm sạch & chuẩn hoá
- Ghi SILVER theo ngày (parquet)
YÊU CẦU: đã có spark_config.get_spark_session()
"""

import os
import re
from datetime import datetime
from typing import Tuple

from dags.code.spark_session.spark_config import get_spark_session
from pyspark.sql import functions as F, types as T


# ========================
# ENV
# ========================
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")
RUN_DATE      = os.getenv("RUN_DATE", datetime.now().strftime("%Y-%m-%d"))


# ========================
# HELPERS
# ========================
def _vn_norm_py(s: str) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _bus_norm_py(s: str) -> str:
    s = _vn_norm_py(s)
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _split_route_py(route: str) -> Tuple[str, str]:
    if not route:
        return "", ""
    parts = re.split(r"\s*-\s*|\s*→\s*", route)
    if len(parts) >= 2:
        return parts[0].strip(), parts[-1].strip()
    return route.strip(), ""

def _parse_price_py(v: str) -> int:
    if v is None:
        return None
    s = str(v).replace(".", "").replace(",", "").replace("đ", "").replace("₫", "").strip()
    return int(s) if s.isdigit() else None

def _parse_duration_minutes_py(v: str) -> int:
    if not v:
        return None
    # hỗ trợ '3h45m', '3h', '3 giờ 45 phút'
    h = re.search(r"(\d+)\s*(?:h|giờ)", v)
    m = re.search(r"(\d+)\s*(?:m|phút)", v)
    hours = int(h.group(1)) if h else 0
    mins  = int(m.group(1)) if m else 0
    return hours * 60 + mins if (hours or mins) else None

def _parse_time_to_ts_py(date_str: str, hm: str) -> str:
    if not hm:
        return None
    hm = hm.strip()
    try:
        dt = datetime.strptime(f"{date_str} {hm}", "%Y-%m-%d %H:%M")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


# UDFs
bus_norm_udf = F.udf(_bus_norm_py, T.StringType())
split_o_udf  = F.udf(lambda r: _split_route_py(r)[0], T.StringType())
split_d_udf  = F.udf(lambda r: _split_route_py(r)[1], T.StringType())
price_udf    = F.udf(_parse_price_py, T.IntegerType())
dep_ts_udf   = F.udf(lambda t: _parse_time_to_ts_py(RUN_DATE, t), T.StringType())
arr_ts_udf   = F.udf(lambda t: _parse_time_to_ts_py(RUN_DATE, t), T.StringType())
dur_udf      = F.udf(_parse_duration_minutes_py, T.IntegerType())


# ========================
# TICKET -> SILVER
# ========================
def ticket_to_silver(spark, run_date: str = RUN_DATE):
    bronze_path = f"s3a://{BRONZE_BUCKET}/ticket/date={run_date}/bronze_ticket_{run_date}.parquet"
    df = spark.read.parquet(bronze_path)

    # Cột bắt buộc tối thiểu
    required = [
        "Bus_Name","Start_Date","Route","Departure_Time","Arrival_Time",
        "Departure_Place","Arrival_Place","Duration","Seat_Types","Seat_Available","Price","Ticket_Sold"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Ticket BRONZE thiếu cột: {missing}")

    # Chuẩn hoá chính
    out = (
        df
        .withColumn("bus_name_norm", bus_norm_udf(F.col("Bus_Name")))
        .withColumn("date", F.lit(run_date))  # chuẩn: dùng RUN_DATE nhất quán
        .withColumn("origin", split_o_udf(F.col("Route")))
        .withColumn("destination", split_d_udf(F.col("Route")))
        .withColumn("departure_ts", dep_ts_udf(F.col("Departure_Time")))
        .withColumn("arrival_ts",   arr_ts_udf(F.col("Arrival_Time")))
        .withColumn("duration_min", dur_udf(F.col("Duration")))
        .withColumn("price_vnd",    price_udf(F.col("Price")))
    )

    # Seats_Left từ Seat_Available
    seats_left = F.regexp_extract(F.lower(F.col("Seat_Available")), r"(\d+)", 1)
    out = out.withColumn(
        "Seats_Left",
        F.when(seats_left == "", F.lit(None).cast("int")).otherwise(seats_left.cast("int"))
    )

    # Capacity từ Seat_Types (tìm số trước 'chỗ|giường|phòng' nếu có)
    cap_num = F.regexp_extract(F.lower(F.col("Seat_Types")), r"(\d+)\s*(?:ch[oô]|\bchỗ\b|gi[ưu]ờng|ph[òo]ng)", 1)
    out = out.withColumn(
        "Capacity",
        F.when(cap_num == "", F.lit(None).cast("int")).otherwise(cap_num.cast("int"))
    )

    # Ticket_Sold: giữ giá trị có sẵn, điền thiếu bằng Capacity - Seats_Left
    out = out.withColumn(
        "Ticket_Sold",
        F.when(F.col("Ticket_Sold").isNotNull(), F.col("Ticket_Sold").cast("int"))
         .when(F.col("Capacity").isNotNull() & F.col("Seats_Left").isNotNull(),
               (F.col("Capacity") - F.col("Seats_Left")).cast("int"))
         .otherwise(F.lit(None).cast("int"))
    )

    # Sold-out flag
    sold_txt = F.lower(F.col("Seat_Available"))
    out = out.withColumn(
        "sold_out_flag",
        F.when(F.col("Seats_Left") == 0, True)
         .when(sold_txt.contains("hết chỗ") | sold_txt.contains("sold out"), True)
         .otherwise(False)
    )

    # Sanity bounds
    out = (
        out
        .withColumn(
            "Ticket_Sold",
            F.when((F.col("Capacity").isNotNull()) & (F.col("Ticket_Sold") > F.col("Capacity")),
                   F.col("Capacity")).otherwise(F.col("Ticket_Sold"))
        )
        .withColumn(
            "Seats_Left",
            F.when((F.col("Capacity").isNotNull()) & (F.col("Seats_Left") > F.col("Capacity")),
                   F.col("Capacity")).otherwise(F.col("Seats_Left"))
        )
    )

    silver_path = f"s3a://{SILVER_BUCKET}/ticket/date={run_date}/silver_ticket_{run_date}.parquet"
    (
        out
        .repartition(1)
        .write.mode("overwrite")
        .parquet(silver_path)
    )


# ========================
# FACILITY -> SILVER
# ========================
def facility_to_silver(spark, run_date: str = RUN_DATE):
    bronze_path = f"s3a://{BRONZE_BUCKET}/facility/date={run_date}/bronze_facility_{run_date}.parquet"
    df = spark.read.parquet(bronze_path)

    for c in ["Bus_Name", "Facilities"]:
        if c not in df.columns:
            raise KeyError(f"Facility BRONZE thiếu cột: {c}")

    to_array = F.udf(
        lambda x: x if isinstance(x, list) else ([] if x in (None, "") else [str(x)]),
        T.ArrayType(T.StringType())
    )

    out = (
        df
        .withColumn("bus_name_norm", bus_norm_udf(F.col("Bus_Name")))
        .withColumn("event_date", F.lit(run_date))
        .withColumn("Facilities", to_array(F.col("Facilities")))
        .withColumn("Facility", F.explode("Facilities"))
        .drop("Facilities")
        .dropna(subset=["Facility"])
        .dropDuplicates(["bus_name_norm", "event_date", "Facility"])
    )

    silver_path = f"s3a://{SILVER_BUCKET}/facility/date={run_date}/silver_facility_{run_date}.parquet"
    (
        out
        .repartition(1)
        .write.mode("overwrite")
        .parquet(silver_path)
    )


# ========================
# REVIEW (+sentiment) -> SILVER
# ========================
def review_to_silver(spark, run_date: str = RUN_DATE):
    bronze_review_path = f"s3a://{BRONZE_BUCKET}/review/date={run_date}/bronze_reviews_{run_date}.parquet"
    scored_path        = f"s3a://{SILVER_BUCKET}/review/date={run_date}/reviews_scored_{run_date}.parquet"

    df_raw = spark.read.parquet(bronze_review_path)
    try:
        df_scored = spark.read.parquet(scored_path)
    except Exception:
        print("⚠️ Chưa có sentiment_scored — bỏ qua review_to_silver.")
        return

    def _pick(cols, *cands):
        for c in cands:
            if c in cols: return c
        return None

    # columns ở bronze
    r_bus  = _pick(df_raw.columns, "Bus_Name", "bus_name")
    r_comm = _pick(df_raw.columns, "Comment", "comment")
    if not (r_bus and r_comm):
        raise KeyError("Review BRONZE thiếu Bus_Name/Comment")

    # columns ở scored (có lang do sentiment sinh)
    s_bus  = _pick(df_scored.columns, "Bus_Name", "bus_name")
    s_comm = _pick(df_scored.columns, "Comment", "comment")
    s_lang = _pick(df_scored.columns, "lang")
    if not (s_bus and s_comm):
        raise KeyError("Review SCORED thiếu Bus_Name/Comment")
    if not s_lang:
        raise KeyError("Review SCORED thiếu lang (hãy giữ lang trong sentiment output)")

    trim = F.udf(lambda x: (x or "").strip(), T.StringType())

    # ❗ KHÔNG tạo lang từ bronze
    raw_norm = (
        df_raw
        .withColumn("bus_name_norm", bus_norm_udf(F.col(r_bus)))
        .withColumn("comment", trim(F.col(r_comm)))
        .withColumn("event_date", F.lit(run_date))
        .withColumn("Bus_Name", F.col(r_bus))
        .select("bus_name_norm", "comment", "event_date", "Bus_Name")
        .filter(F.length(F.col("comment")) > 0)
    )

    # lấy lang từ scored
    sc_norm = (
        df_scored
        .withColumn("bus_name_norm", bus_norm_udf(F.col(s_bus)))
        .withColumn("comment", trim(F.col(s_comm)))
        .withColumn("event_date", F.lit(run_date))
        .withColumn("lang", F.col(s_lang))
        .select(
            "bus_name_norm", "comment", "lang", "event_date",
            "NEG", "NEU", "POS", "sentiment_score", "sentiment_label"
        )
        .filter(F.length(F.col("comment")) > 0)
    )

    # join KHÔNG dùng r_lang; ghép theo {bus_name_norm, comment, event_date}
    joined = (
        raw_norm.alias("r")
        .join(sc_norm.alias("s"),
              on=["bus_name_norm", "comment", "event_date"],
              how="inner")
        .select(
            "r.Bus_Name", "r.bus_name_norm", "r.comment", "s.lang", "r.event_date",
            "NEG", "NEU", "POS", "sentiment_score", "sentiment_label"
        )
        .dropDuplicates(["bus_name_norm", "comment", "lang", "event_date"])
    )

    silver_path = f"s3a://{SILVER_BUCKET}/review/date={run_date}/silver_reviews_{run_date}.parquet"
    (
        joined
        .repartition(1)
        .write.mode("overwrite")
        .parquet(silver_path)
    )


# ========================
# MAIN
# ========================
def run_all():
    spark = get_spark_session("to_silver")
    ticket_to_silver(spark, RUN_DATE)
    facility_to_silver(spark, RUN_DATE)
    review_to_silver(spark, RUN_DATE)
    print("✅ to_silver done")

if __name__ == "__main__":
    run_all()
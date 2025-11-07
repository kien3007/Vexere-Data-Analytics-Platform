import os
import sys
import json
from glob import glob
from datetime import datetime
import pandas as pd

# ---- RAW local (host bind-mount) ----
RAW_DIR = os.getenv("LOCAL_RAW_DIR", "/data/raw")

# ---- BRONZE trên MinIO (S3) ----
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")  # tên bucket để chứa BRONZE
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# S3 storage options for fsspec/s3fs
S3_OPTS = {
    "key": AWS_KEY,
    "secret": AWS_SECRET,
    "client_kwargs": {"endpoint_url": S3_ENDPOINT},
}

def _read_jsonl(path: str) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        rows = [json.loads(l) for l in f if l.strip()]
    return pd.DataFrame(rows)

def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")

def convert(file_type: str = "ticket", date_tag: str | None = None) -> bool:
    """
    Convert RAW (LOCAL) -> BRONZE (MinIO S3 Parquet)
      - file_type: 'ticket' | 'facility' | 'review'
      - date_tag: 'YYYY-MM-DD' (mặc định: hôm nay)
    """
    date_tag = date_tag or _today()

    if file_type == "ticket":
        # RAW CSV trên host
        src_dir = f"{RAW_DIR}/ticket/date={date_tag}"
        src = f"{src_dir}/bus_ticket_{date_tag}.csv"
        if not os.path.isfile(src):
            cand = sorted(glob(f"{src_dir}/*.csv"))
            if cand:
                src = cand[-1]
        if not os.path.isfile(src):
            print(f"❌ Không tìm thấy RAW ticket cho ngày {date_tag}: {src_dir}")
            return False

        # BRONZE Parquet trên MinIO
        dst = f"s3://{BRONZE_BUCKET}/ticket/date={date_tag}/bronze_ticket_{date_tag}.parquet"

        print(f"📥 Đọc RAW ticket: {src}")
        df = pd.read_csv(src)
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    elif file_type == "facility":
        # chỉnh: đúng tên file daily của crawler facility
        src = f"{RAW_DIR}/facility/date={date_tag}/bus_facility_{date_tag}.jsonl"
        if not os.path.isfile(src):
            print(f"❌ Không tìm thấy RAW facility: {src}")
            return False

        dst = f"s3://{BRONZE_BUCKET}/facility/date={date_tag}/bronze_facility_{date_tag}.parquet"

        print(f"📥 Đọc RAW facility: {src}")
        df = _read_jsonl(src)
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    elif file_type == "review":
        # chỉnh: đúng thư mục 'review' (không 'reviews') và tên file daily
        src = f"{RAW_DIR}/review/date={date_tag}/bus_reviews_{date_tag}.jsonl"
        if not os.path.isfile(src):
            print(f"❌ Không tìm thấy RAW reviews: {src}")
            return False

        # chỉnh: path đích cũng là 'review'
        dst = f"s3://{BRONZE_BUCKET}/review/date={date_tag}/bronze_reviews_{date_tag}.parquet"

        print(f"📥 Đọc RAW reviews: {src}")
        df = _read_jsonl(src)
        df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    else:
        print(f"❌ file_type không hợp lệ: {file_type}")
        return False

    # Ghi Parquet lên MinIO (S3) qua s3fs
    print(f"💾 Ghi BRONZE → {dst}")
    df.to_parquet(dst, index=False, storage_options=S3_OPTS)
    print(f"✅ DONE {file_type} @ {date_tag}")
    return True

if __name__ == "__main__":
    # CLI:
    #   python to_brz.py ticket 2025-10-30
    #   python to_brz.py facility
    #   python to_brz.py review 2025-10-29
    ftype = sys.argv[1] if len(sys.argv) >= 2 else "ticket"
    datearg = sys.argv[2] if len(sys.argv) >= 3 else None
    ok = convert(ftype, datearg)
    sys.exit(0 if ok else 1)
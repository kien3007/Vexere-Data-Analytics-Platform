# -*- coding: utf-8 -*-
import os
from datetime import datetime
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import re

# ======================
# MinIO / S3 config
# ======================
S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://minio:9000")
AWS_KEY       = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET    = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "silver")

S3_OPTS = {
    "key": AWS_KEY,
    "secret": AWS_SECRET,
    "client_kwargs": {"endpoint_url": S3_ENDPOINT},
}

# ======================
# Models
# ======================
MODEL_VI    = "mr4/phobert-base-vi-sentiment-analysis"                    # 3 lớp: NEG/NEU/POS
MODEL_MULTI = "cardiffnlp/twitter-xlm-roberta-base-sentiment"  # 3 lớp: NEG/NEU/POS

# ======================
# Runtime tuning
# ======================
BATCH_SIZE = int(os.getenv("SENT_BATCH_SIZE", "16"))
TH_POS = float(os.getenv("SENT_TH_POS", "0.20"))
TH_NEG = float(os.getenv("SENT_TH_NEG", "0.20"))

# Cache HF (khớp compose: user 'airflow')
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ.setdefault("HF_HOME", "/home/airflow/.cache/huggingface")
os.environ.setdefault("TRANSFORMERS_CACHE", "/home/airflow/.cache/huggingface")

_tok_vi = _mdl_vi = _tok_multi = _mdl_multi = None

def _load_models_once():
    """Load 2 model một lần (lazy)."""
    global _tok_vi, _mdl_vi, _tok_multi, _mdl_multi
    if _tok_vi is None:
        _tok_vi  = AutoTokenizer.from_pretrained(MODEL_VI)
        _mdl_vi  = AutoModelForSequenceClassification.from_pretrained(MODEL_VI)
        _mdl_vi.eval()
    if _tok_multi is None:
        _tok_multi = AutoTokenizer.from_pretrained(MODEL_MULTI)
        _mdl_multi = AutoModelForSequenceClassification.from_pretrained(MODEL_MULTI)
        _mdl_multi.eval()

def _score_batch_3cls(texts, tok, mdl):
    """Trả về list dict với keys ['NEG','NEU','POS'] (3 lớp)."""
    outs = []
    if len(texts) == 0:
        return outs
    mdl.eval()
    with torch.no_grad():
        for i in range(0, len(texts), BATCH_SIZE):
            chunk = [(t or "").strip() for t in texts[i:i+BATCH_SIZE]]
            enc = tok(
                chunk,
                truncation=True,
                max_length=256,
                padding=True,
                return_tensors="pt"
            )
            logits = mdl(**enc).logits
            probs = torch.softmax(logits, dim=-1).tolist()
            for p in probs:
                outs.append({"NEG": float(p[0]), "NEU": float(p[1]), "POS": float(p[2])})
    return outs

def _finalize(df: pd.DataFrame) -> pd.DataFrame:
    """Thêm score & label chuẩn hóa."""
    if df is None or df.empty:
        return df
    for c in ["NEG", "NEU", "POS"]:
        if c not in df.columns:
            df[c] = 0.0
    df["sentiment_score"] = df["POS"] - df["NEG"]
    def label_fn(s: float) -> str:
        if s > TH_POS:
            return "POS"
        if s < -TH_NEG:
            return "NEG"
        return "NEU"
    df["sentiment_label"] = df["sentiment_score"].map(label_fn)
    return df

def run_sentiment() -> str:
    """Đọc BRONZE review hằng ngày, chấm VI & MULTI, hợp nhất ghi về SILVER."""
    _load_models_once()
    date_tag = datetime.now().strftime("%Y-%m-%d")

    src = f"s3://{BRONZE_BUCKET}/review/date={date_tag}/bronze_reviews_{date_tag}.parquet"
    dst = f"s3://{SILVER_BUCKET}/review/date={date_tag}/reviews_scored_{date_tag}.parquet"

    print(f"📥 Loading BRONZE → {src}")
    df = pd.read_parquet(src, storage_options=S3_OPTS)

    if df is None or df.empty:
        print("ℹ️ BRONZE review trống, ghi SILVER rỗng.")
        pd.DataFrame().to_parquet(dst, index=False, storage_options=S3_OPTS)
        print(f"💾 Wrote empty SILVER → {dst}")
        return dst

    # Cột comment: giữ đúng case từ crawler
    comment_col = "Comment" if "Comment" in df.columns else ("comment" if "comment" in df.columns else None)
    if comment_col is None:
        raise KeyError("Không tìm thấy cột Comment/comment trong dữ liệu review BRONZE")

    # regex ký tự có dấu tiếng Việt
    _VI_RE = re.compile(r"[ăâđêôơưáàảãạăắằẳẵặấầẩẫậéèẻẽẹếềểễệóòỏõọốồổỗộớờởỡợíìỉĩịúùủũụứừửữựýỳỷỹỵ]", re.IGNORECASE)

    def _detect_lang_fast(text: str) -> str:
        if text is None:
            return "en"
        t = str(text).lower()
    # nếu có dấu → tiếng Việt
        if _VI_RE.search(t):
            return "vi"
        return "en"

    # --- Tạo cột lang (không cần langdetect) ---
    df["lang"] = df[comment_col].astype(str).map(_detect_lang_fast)
    
    vi_mask    = df["lang"].eq("vi")
    multi_mask = ~vi_mask

    parts = []

    # VI only
    if vi_mask.any():
        vi_texts  = df.loc[vi_mask, comment_col].tolist()
        vi_scores = _score_batch_3cls(vi_texts, _tok_vi, _mdl_vi)
        vi_df = df.loc[vi_mask].copy()
        if vi_scores:
            vi_df["NEG"] = [x["NEG"] for x in vi_scores]
            vi_df["NEU"] = [x["NEU"] for x in vi_scores]
            vi_df["POS"] = [x["POS"] for x in vi_scores]
        parts.append(vi_df)

    # Multi-lang
    if multi_mask.any():
        ml_texts  = df.loc[multi_mask, comment_col].tolist()
        ml_scores = _score_batch_3cls(ml_texts, _tok_multi, _mdl_multi)
        ml_df = df.loc[multi_mask].copy()
        if ml_scores:
            ml_df["NEG"] = [x["NEG"] for x in ml_scores]
            ml_df["NEU"] = [x["NEU"] for x in ml_scores]
            ml_df["POS"] = [x["POS"] for x in ml_scores]
        parts.append(ml_df)

    if parts:
        df_out = pd.concat(parts, ignore_index=True)
    else:
        # Không match mask nào, trả nguyên df (không có score)
        df_out = df.copy()

    df_out = _finalize(df_out)

    print(f"💾 Writing SILVER → {dst}")
    df_out.to_parquet(dst, index=False, storage_options=S3_OPTS)

    print("✅ Sentiment completed (unified output).")
    return dst

if __name__ == "__main__":
    run_sentiment()
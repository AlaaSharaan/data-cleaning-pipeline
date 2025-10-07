import pandas as pd
from datetime import datetime, timedelta

def extract_report_date_from_df(df: pd.DataFrame):
    date_col = next((c for c in df.columns if str(c).strip().lower() == "date"), None)
    if date_col is None:
        return (datetime.now() - timedelta(days=1)).date()
    s = df[date_col].astype(str).str.strip()

    fmts = ["%m/%d/%Y","%d/%m/%Y","%Y-%m-%d","%m-%d-%Y","%d-%m-%Y",
            "%m.%d.%Y","%d.%m.%Y","%m/%d/%Y %H:%M","%d/%m/%Y %H:%M",
            "%m/%d/%Y %H:%M:%S","%d/%m/%Y %H:%M:%S"]
    for fmt in fmts:
        dt = pd.to_datetime(s, format=fmt, errors="coerce")
        if dt.notna().any():
            return dt.max().date()

    for dayfirst in (True, False):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst)
        if dt.notna().any():
            return dt.max().date()

    ser = pd.to_numeric(s, errors="coerce")
    if ser.notna().any():
        base = datetime(1899, 12, 30)
        return (base + pd.to_timedelta(ser.max(), unit="D")).date()

    return (datetime.now() - timedelta(days=1)).date()

def map_and_prepare(raw_df, ref_df, raw_key_col, ref_key_col, account_col):
    mapping = (
        ref_df.drop_duplicates(subset=[ref_key_col], keep="last")
              .set_index(ref_key_col)[account_col]
              .to_dict()
    )
    work = raw_df.copy()
    work[account_col] = work[raw_key_col].map(mapping)
    work = work.dropna(subset=[account_col])
    work = work.rename(columns={raw_key_col: "skill"})
    return work

def split_by_account(work_df, account_col, dedup_keys):
    for grp in sorted(work_df[account_col].dropna().unique()):
        gdf = (
            work_df[work_df[account_col] == grp]
            .drop(columns=[account_col], errors="ignore")
            .copy()
        )
        # نثبت المفاتيح قبل إزالة التكرار
        for c in dedup_keys:
            if c in gdf.columns:
                gdf[c] = gdf[c].astype(str).str.strip()
        gdf.drop_duplicates(subset=dedup_keys, keep="first", inplace=True)
        yield grp, gdf

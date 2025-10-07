from pathlib import Path
import os, csv, shutil
import pandas as pd
from datetime import datetime

# --------------------------------------
# إعدادات الامتدادات المسموح بها
# --------------------------------------
EXCEL_EXTS = {".xlsx", ".xlsm"}
TSV_IN_XLS = {".xls"}      # ملفات نصية مفصولة بـ TAB لكنها محفوظة بامتداد .xls
CSV_EXTS   = {".csv", ".tsv", ".txt"}

# أولوية اختيار الملفات (الأعلى = الأفضل)
PRIORITY = {
    ".tsv": 3,
    ".csv": 2,
    ".xlsx": 1,
    ".xlsm": 1,
    ".txt": 1,
    ".xls": 0,
}

# --------------------------------------
# البحث عن أحدث ملف في مجلد محدد
# --------------------------------------
def find_latest_file(folder: Path):
    files = [
        p for p in folder.glob("*")
        if p.suffix.lower() in (EXCEL_EXTS | TSV_IN_XLS | CSV_EXTS)
    ]
    if not files:
        return None

    # الترتيب حسب وقت التعديل وأولوية الامتداد
    files.sort(key=lambda p: (p.stat().st_mtime, PRIORITY.get(p.suffix.lower(), 0)), reverse=True)
    return files[0]

# --------------------------------------
# قراءة أي ملف CSV/TSV/Excel (بأمان)
# --------------------------------------
def safe_read_any(path):
    path = Path(path)
    suf = path.suffix.lower()

    # تحديد الفاصل
    sep = "\t" if suf in (".tsv", ".txt", ".xls") else ","

    # ترتيب الترميزات حسب نوع الملف
    encodings = ["utf-8-sig", "utf-8", "cp1256", "utf-16"]
    if suf == ".xls":  # ملفات .xls عندنا نصية UTF-16
        encodings = ["utf-16", "utf-8-sig", "utf-8", "cp1256"]

    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, sep=sep, dtype=str, engine="python", encoding=enc)
        except UnicodeDecodeError as e:
            last_err = e
            continue

    # محاولة أخيرة بدون تحديد ترميز (مش مفضل لكن كخطة طوارئ)
    try:
        return pd.read_csv(path, sep=sep, dtype=str, engine="python", encoding=None)
    except Exception:
        if last_err:
            raise last_err
        raise

# --------------------------------------
# نقل نسخة أرشيفية للملف
# --------------------------------------
def archive_copy(src: Path, archive_base: Path, rep_date) -> Path:
    month_dir = archive_base / f"{rep_date:%m.%Y}"
    month_dir.mkdir(parents=True, exist_ok=True)
    date_str = f"{rep_date.day}.{rep_date.month}.{rep_date.year}"
    dest = month_dir / f"daily_report{date_str}{src.suffix}"

    i, final = 2, dest
    while final.exists():
        final = month_dir / f"daily_report{date_str}_v{i}{src.suffix}"
        i += 1

    shutil.copy2(src, final)
    return final

# --------------------------------------
# توحيد المفاتيح (قبل دمج البيانات)
# --------------------------------------
def _normalize_keys(df: pd.DataFrame, keys):
    for c in keys:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

# --------------------------------------
# كتابة CSV بدون تكرارات
# --------------------------------------
def write_csv_no_duplicates(csv_path: Path, new_df: pd.DataFrame, dedup_keys):
    new_df = _normalize_keys(new_df, dedup_keys)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    if csv_path.exists() and csv_path.stat().st_size > 0:
        old_df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    else:
        old_df = pd.DataFrame()

    before = len(old_df)
    incoming = len(new_df)

    merged = pd.concat([old_df, new_df], ignore_index=True)
    merged.drop_duplicates(subset=dedup_keys, keep="first", inplace=True)

    after = len(merged)
    added = max(after - before, 0)
    total_dups_removed = before + incoming - after

    tmp = csv_path.with_suffix(csv_path.suffix + ".tmp")
    merged.to_csv(tmp, index=False, quoting=csv.QUOTE_MINIMAL, encoding="utf-8-sig")
    tmp.replace(csv_path)

    return {
        "added": added,
        "dups_removed": max(total_dups_removed, 0),
        "final_rows": after,
    }

# --------------------------------------
# حذف ملفات CSV القديمة في مجلد محدد
# --------------------------------------
def purge_old_group_csvs(folder: Path) -> int:
    count = 0
    for fp in folder.glob("*_data.csv"):
        try:
            fp.unlink()
            count += 1
        except Exception:
            pass
    return count

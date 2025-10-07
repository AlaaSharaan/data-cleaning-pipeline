# generate_fake_data.py
# Fake CMS-like data generator: 7 days * 96 intervals/day * multiple skills
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time

# -----------------------
# Settings (edit if you like)
# -----------------------
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "sample_data" / "raw_cms"
REF_DIR = BASE_DIR / "sample_data" / "reference"
N_DAYS = 7          # عدد الأيام
INTERVALS_PER_DAY = 96  # 96 = كل 15 دقيقة
SEED = 42

# مهارات وهمية + ماب للأكونت (عدّل اللي يعجبك)
SKILLS = [
    "BlueNet_Premium_EN",
    "BlueNet_Medium_AR",
    "BlueNet_Install_EN",
    "FiberLink_Support_AR",
    "FiberLink_Upgrade_EN",
    "FiberLink_Billing_AR",
    "AcmeMobile_Inbound_EN",
    "AcmeMobile_Troubleshoot_AR",
    "AcmeMobile_Device_EN",
    "SigmaTech_Core_EN",
    "SigmaTech_Field_AR",
    "SigmaTech_Escalations_EN",
    "CityCare_General_EN",
    "CityCare_Billing_AR",
    "CityCare_Retention_EN",
    "PayWave_Payments_AR",
    "PayWave_Disputes_EN",
    "PayWave_Refunds_AR",
    "BizCloud_B2B_Onboarding_EN",
    "BizCloud_B2B_SLA_EN",
    "DataWorks_B2B_Accounts_AR",
    "DataWorks_B2B_Projects_EN",
    "DataWorks_B2B_Support_AR",
    "DataWorks_B2B_NOC_EN",
]

ACCOUNT_MAP = {
    # BlueNet
    "BlueNet_Premium_EN": "BlueNet",
    "BlueNet_Medium_AR": "BlueNet",
    "BlueNet_Install_EN": "BlueNet",
    "FiberLink_Support_AR": "BlueNet",
    "FiberLink_Upgrade_EN": "BlueNet",
    "FiberLink_Billing_AR": "BlueNet",
    # Sigma
    "AcmeMobile_Inbound_EN": "Sigma",
    "AcmeMobile_Troubleshoot_AR": "Sigma",
    "AcmeMobile_Device_EN": "Sigma",
    "SigmaTech_Core_EN": "Sigma",
    "SigmaTech_Field_AR": "Sigma",
    "SigmaTech_Escalations_EN": "Sigma",
    # PayWave
    "CityCare_General_EN": "PayWave",
    "CityCare_Billing_AR": "PayWave",
    "CityCare_Retention_EN": "PayWave",
    "PayWave_Payments_AR": "PayWave",
    "PayWave_Disputes_EN": "PayWave",
    "PayWave_Refunds_AR": "PayWave",
    # Business
    "BizCloud_B2B_Onboarding_EN": "B2B",
    "BizCloud_B2B_SLA_EN": "B2B",
    "DataWorks_B2B_Accounts_AR": "B2B",
    "DataWorks_B2B_Projects_EN": "B2B",
    "DataWorks_B2B_Support_AR": "B2B",
    "DataWorks_B2B_NOC_EN": "B2B",
}

# -----------------------
# Helpers
# -----------------------
def ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REF_DIR.mkdir(parents=True, exist_ok=True)

def make_time_series():
    # 00:00:00 → 23:45:00 كل 15 دقيقة
    minutes = np.arange(0, 24 * 60, 15)
    return [time(m // 60, m % 60, 0).strftime("%H:%M:%S") for m in minutes]

def daily_profile(base_calls: int, jitter: float = 0.15):
    """
    شكل بسيط لكيرف اليوم: هدوء بالليل، قمة منتصف اليوم، نزول تدريجي.
    بيرجع 96 نقطة (لكل إنترفال) كنسبة من base_calls.
    """
    x = np.linspace(0, 2 * np.pi, INTERVALS_PER_DAY, endpoint=False)
    curve = 0.55 + 0.45 * np.sin(x - np.pi / 2)  # بين ~0.1 و 1.0
    noise = np.random.normal(0, jitter, size=curve.size)
    curve = np.clip(curve + noise, 0.10, None)
    q = (curve / curve.sum()) * base_calls
    return np.round(q).astype(int)

def build_day_df(day: datetime.date) -> pd.DataFrame:
    rows = []
    times = make_time_series()

    for skill in SKILLS:
        acc = ACCOUNT_MAP[skill]
        # بدّل الفروع لتطابق الـ accounts الفعلية
        if acc == "BlueNet":
            base = np.random.randint(6000, 14000)
            aht_mu = 360
        elif acc == "Sigma":
            base = np.random.randint(4000, 9000)
            aht_mu = 410
        elif acc == "PayWave":
            base = np.random.randint(3000, 8000)
            aht_mu = 380
        else:  # B2B
            base = np.random.randint(3000, 8000)
            aht_mu = 420

        calls_series = daily_profile(base)
        aht_series = np.clip(np.random.normal(aht_mu, 25, size=INTERVALS_PER_DAY), 300, 480).astype(int)

        sch = np.round((calls_series * aht_series) / 900).astype(int)
        sch = np.clip(sch, 1, None)

        workload = calls_series * aht_series
        capacity = sch * 900
        occ = np.clip(workload / np.maximum(capacity, 1), 0, 2)
        sla_arr = np.clip(100 - (occ - 1) * 35 + np.random.normal(0, 3, size=occ.size), 60, 99)
        sla_arr = np.round(sla_arr, 1)

        aban = np.maximum(np.round(calls_series * np.random.uniform(0.002, 0.01, size=calls_series.size)).astype(int), 0)
        held = np.maximum(np.round(calls_series * np.random.uniform(0.001, 0.006, size=calls_series.size)).astype(int), 0)

        for t, c, aht, s, sl, ab, hd in zip(times, calls_series, aht_series, sch, sla_arr, aban, held):
            rows.append({
                "Date": day.strftime("%m/%d/%Y"),
                "Split/Skill": skill,
                "Time": t,
                "CALLSOFFERED": int(c),
                "AHT": int(aht),
                "SLA %": float(sl),     # ← استخدم قيمة الـ SLA لنفس الصف
                "Sch": int(s),
                "Needed": float(np.round(s, 2)),  # تقدر ترجع needed لو عايز الضوضاء
                "Aban Calls": int(ab),
                "Held Calls": int(hd),
            })
    return pd.DataFrame(rows)


def save_day_files(df, day):
    # ملف TSV نظيف (Excel-friendly)
    tsv_path = RAW_DIR / f"daily_report{day.strftime('%Y-%m-%d')}.tsv"
    df.to_csv(tsv_path, sep="\t", index=False, encoding="utf-8-sig", lineterminator="\n")

    # اختياري: نسخة UTF-16 بامتداد .xls (لعملاء بيستخدموا Excel قديم)
    xls_path = RAW_DIR / f"daily_report{day.strftime('%Y-%m-%d')}.xls"
    with open(xls_path, "w", encoding="utf-16") as f:
        df.to_csv(f, sep="\t", index=False, lineterminator="\n")


def save_reference():
    ref = pd.DataFrame({"SplitSkill": SKILLS, "Account": [ACCOUNT_MAP[s] for s in SKILLS]})
    ref_path = REF_DIR / "SkillAccountMapping.xlsx"
    with pd.ExcelWriter(ref_path, engine="openpyxl") as xw:
        ref.to_excel(xw, index=False, sheet_name="Table1")

def main():
    np.random.seed(SEED)
    ensure_dirs()

    today = datetime.now().date()
    for i in range(N_DAYS):
        day = today - timedelta(days=(N_DAYS - 1 - i))
        df = build_day_df(day)
        save_day_files(df, day)

    save_reference()
    print(f"✔ Done. Generated {N_DAYS} day(s) into: {RAW_DIR}")
    print(f"→ Reference saved to: {REF_DIR / 'SkillAccountMapping.xlsx'}")

if __name__ == "__main__":
    main()

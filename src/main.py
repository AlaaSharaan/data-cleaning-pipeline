from pathlib import Path
import pandas as pd
from config import load_settings
from io_utils import (
    find_latest_file, safe_read_any, archive_copy,
    write_csv_no_duplicates, purge_old_group_csvs
)
from transform import extract_report_date_from_df, map_and_prepare, split_by_account

CHECK, ARROW = "✔", "→"

def main():
    settings = load_settings(Path("config.yaml"))
    print("\n--- Data Cleaning Pipeline ---")

    latest_raw = find_latest_file(Path(settings.raw_data_folder))
    if not latest_raw:
        raise SystemExit(f"No Excel/CSV/TSV files in {settings.raw_data_folder}")
    print(f"{ARROW} Latest raw: {latest_raw.name}")

    raw_df = safe_read_any(latest_raw)
    ref_df = pd.read_excel(settings.reference_file_path, dtype=str)

    rep_date = extract_report_date_from_df(raw_df)
    archived_path = archive_copy(latest_raw, Path(settings.archive_base_dir), rep_date)
    print(f"{ARROW} Archived copy: {archived_path}")

    work_df = map_and_prepare(
        raw_df, ref_df,
        settings.raw_data_key_column,
        settings.reference_key_column,
        settings.skill_group_column,
    )

    out_dir = Path(settings.forecast_csv_folder)
    out_dir.mkdir(parents=True, exist_ok=True)
    purge_old_group_csvs(out_dir)

    tot_added = tot_removed = 0
    for grp, gdf in split_by_account(work_df, settings.skill_group_column, settings.dedup_keys):
        csv_path = out_dir / f"{grp}_data.csv"
        stats = write_csv_no_duplicates(csv_path, gdf, settings.dedup_keys)
        tot_added += stats["added"]
        tot_removed += stats["dups_removed"]
        print(f" {CHECK} {grp:<12} added +{stats['added']:>5}  dups -{stats['dups_removed']:>5}  total {stats['final_rows']}")

    print("\nSummary:")
    print(f"  New rows added: {tot_added}")
    print(f"  Duplicates removed: {tot_removed}")

if __name__ == "__main__":
    main()

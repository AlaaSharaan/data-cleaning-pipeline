from dataclasses import dataclass
import yaml

@dataclass
class Settings:
    raw_data_folder: str
    reference_file_path: str
    forecast_csv_folder: str
    archive_base_dir: str
    reference_key_column: str
    skill_group_column: str
    raw_data_key_column: str
    dedup_keys: list
    enable_excel_macros: bool = False
    excel_macros: list | None = None

def load_settings(path):
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return Settings(**data)

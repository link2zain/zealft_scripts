import os
import glob
import pandas as pd
import requests
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# === Config ===
base_folder = r"C:\work\jpx_japan\JPX_CVs"
JP_EN_MAP = {
    "要素ID": "ElementID",
    "項目名": "ItemName",
    "コンテキストID": "ContextID",
    "相対年度": "RelativeYear",
    "連結・個別": "ConsolidatedOrNot",
    "期間・時点": "PeriodOrPoint",
    "ユニットID": "UnitID",
    "単位": "Unit",
    "値": "Value"
}
company_code_col = "Local Code"
LARAVEL_UPLOAD_URL = "https://zealft.com/api/jpx/upload-report"

TABLES = {
    "annual": "annual",
    "semi-annual": "semi-annual",
    "quarterly": "quarterly"
}

# === Helpers ===
def detect_report_type(path):
    p = path.lower()
    if "semi-annual" in p:
        return "semi-annual"
    elif "quarter" in p or "quarterly" in p:
        return "quarterly"
    elif "annual" in p:
        return "annual"
    return None

def get_local_code_from_path(path):
    parts = path.split(os.sep)
    for i, part in enumerate(parts):
        if part == "JPX_CVs" and i+1 < len(parts):
            return parts[i+1]
    return None

def translate_headers(headers, jp_en_map):
    translated = []
    for h in headers:
        if h in jp_en_map:
            translated.append(jp_en_map[h])
        else:
            # Attempt to auto-translate via Google Translate API
            try:
                url = "https://translate.googleapis.com/translate_a/single"
                params = {"client": "gtx", "sl": "ja", "tl": "en", "dt": "t", "q": h}
                resp = requests.get(url, params=params)
                en = resp.json()[0][0][0]
                translated.append(en.replace(" ", "_"))
            except:
                translated.append(f"col_{headers.index(h)+1}")
    return translated

def send_csv_to_laravel(df, local_code, report_type):
    url = f"{LARAVEL_UPLOAD_URL}/{report_type}"
    df = df.where(pd.notnull(df), None)
    payload = {
        "local_code": local_code,
        "data": df.to_dict(orient="records")
    }
    try:
        res = requests.post(url, json=payload)
        if res.status_code == 200:
            print(f"✅ Uploaded report for {local_code}")
        else:
            print(f"❌ Upload failed for {local_code}: {res.text}")
    except Exception as e:
        print(f"❌ Exception during upload for {local_code}: {e}")

def process_csv(csv_path):
    if not os.path.exists(csv_path):  # ✅ Avoid double processing error
        print(f"⏭️ Skipping missing file: {csv_path}")
        return

    report_type = detect_report_type(csv_path)
    if not report_type:
        print(f"⚠️ Unknown type: {csv_path}")
        return

    local_code = get_local_code_from_path(csv_path)
    if not local_code:
        print(f"⚠️ Could not extract local code from path: {csv_path}")
        return

    try:
        try:
            df = pd.read_csv(csv_path, sep="\t", dtype=str, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, sep="\t", dtype=str, encoding="utf-16")

        df.columns = translate_headers(df.columns, JP_EN_MAP)
        send_csv_to_laravel(df, local_code, report_type)

        finished_path = csv_path + ".finished"
        if os.path.exists(csv_path):  # ✅ Only rename if still exists
            os.rename(csv_path, finished_path)
            print(f"✓ Processed: {finished_path}")
        else:
            print(f"⚠️ File already moved or deleted: {csv_path}")
    except Exception as e:
        print(f"✗ Failed to process {csv_path}: {e}")

# === Watchdog ===
class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith(".csv"):
            return
        process_csv(event.src_path)

# === Main ===
def main():
    # Initial scan
    csv_files = glob.glob(os.path.join(base_folder, "**", "*.csv"), recursive=True)
    for f in csv_files:
        if not f.endswith(".csv.finished"):
            process_csv(f)

    # Start live watch
    observer = Observer()
    observer.schedule(CSVHandler(), base_folder, recursive=True)
    observer.start()
    print("📡 Watching for new CSV files... Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()

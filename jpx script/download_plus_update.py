import os
import time
import random
import threading
import queue
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import zipfile
import sqlite3
from datetime import datetime

# === Config ===
base_folder = r"C:\work\jpx_japan\JPX_CVs"
CODES_API_URL = "https://zealft.com/api/jpx/codes"
UPDATE_API_URL = "https://zealft.com/api/jpx/report-download"
DB_PATH = os.path.join(base_folder, "jpx_processing.db")

# === Database Setup ===
def init_db():
    os.makedirs(base_folder, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_codes (
            code TEXT PRIMARY KEY,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def is_code_processed(code):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM processed_codes WHERE code = ?", (code,))
    result = cursor.fetchone()
    conn.close()
    return result is not None and result[0] == "completed"

def mark_code_processed(code, status):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.utcnow()
    cursor.execute("""
        INSERT OR REPLACE INTO processed_codes (code, status, created_at, updated_at)
        VALUES (?, ?, ?, ?)
    """, (code, status, now, now))
    conn.commit()
    conn.close()

def update_code_timestamp(code):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now = datetime.utcnow()
    cursor.execute("""
        UPDATE processed_codes 
        SET updated_at = ?
        WHERE code = ?
    """, (now, code))
    conn.commit()
    conn.close()

# === Setup Chrome options ===
chrome_options = Options()
prefs = {
    "download.default_directory": base_folder,
    "download.prompt_for_download": False,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

# === Human behavior functions ===
def human_pause(min_sec, max_sec):
    time.sleep(random.uniform(min_sec, max_sec))

def random_scroll(driver):
    scroll_height = random.randint(100, 800)
    direction = random.choice([-1, 1])
    driver.execute_script(f"window.scrollBy(0, {direction * scroll_height});")
    human_pause(0.5, 2.0)

def random_mouse_move(driver):
    elements = driver.find_elements(By.XPATH, "//*")
    if elements:
        elem = random.choice(elements)
        try:
            ActionChains(driver).move_to_element(elem).perform()
            human_pause(0.2, 1.0)
        except:
            pass

# === File unzipper ===
def unzip_worker(zip_queue):
    while True:
        zip_path = zip_queue.get()
        if zip_path is None:
            break
        try:
            folder = os.path.dirname(zip_path)
            zip_base = os.path.splitext(os.path.basename(zip_path))[0]
            target_folder = os.path.join(folder, zip_base)
            os.makedirs(target_folder, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(target_folder)
            print(f"Unzipped: {zip_path} to {target_folder}")
            os.remove(zip_path)
            code = os.path.basename(os.path.dirname(os.path.dirname(zip_path)))
            mark_code_processed(code, "completed")
        except Exception as e:
            print(f"Failed to unzip {zip_path}: {e}")
            code = os.path.basename(os.path.dirname(os.path.dirname(zip_path)))
            mark_code_processed(code, "failed")
        zip_queue.task_done()

# === API helpers ===
def fetch_codes_from_api():
    try:
        response = requests.get(CODES_API_URL)
        response.raise_for_status()
        data = response.json()
        codes = data.get("codes", [])
        return [code for code in codes if not is_code_processed(code)]
    except Exception as e:
        print(f"❌ Error fetching codes from API: {e}")
        return []

def post_result_to_api(code, found):
    try:
        response = requests.post(UPDATE_API_URL, json={
            "code": code,
            "data_found": 1 if found else 0,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        })
        if response.status_code != 200:
            print(f"❌ Failed to update code {code}: {response.text}")
    except Exception as e:
        print(f"❌ Error posting result for {code}: {e}")

# === Main downloader ===
def download_worker(zip_queue):
    init_db()
    codes = fetch_codes_from_api()
    if not codes:
        print("❌ No unprocessed codes received from API.")
        return

    print("Starting Chrome WebDriver...")
    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()
    print("WebDriver started.")

    for idx_code, code in enumerate(codes):
        print(f"\n🔎 Processing Local_Code: {code}")
        mark_code_processed(code, "processing")
        found = False
        try:
            driver.get("https://disclosure2.edinet-fsa.go.jp/week0020.aspx")
            human_pause(3, 6)
            random_scroll(driver)
            random_mouse_move(driver)
            human_pause(1, 2)

            search_input = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "W0018vD_KEYWORD_WEEE0040"))
            )
            update_code_timestamp(code)
            search_input.clear()
            search_input.send_keys(code)
            human_pause(0.8, 2.0)
            driver.find_element(By.ID, "W0018BTNBTN_SERACH").click()
            human_pause(5, 8)

            csv_links = driver.find_elements(By.XPATH, "//a[contains(text(),'CSV') and starts-with(@onclick,'javascript:Weee0040CsvClick')]")
            print(f"    📄 Found {len(csv_links)} CSV link(s)")
            if csv_links:
                found = True

            for idx, link in enumerate(csv_links):
                try:
                    tr = link.find_element(By.XPATH, "./ancestor::tr[1]")
                    doc_td = tr.find_elements(By.TAG_NAME, "td")[1]
                    doc_text = doc_td.text.strip().lower()

                    if "quarter" in doc_text:
                        report_type = "Quarterly"
                    elif "semi" in doc_text or "interim" in doc_text:
                        report_type = "SemiAnnual"
                    elif "annual" in doc_text or "financial" in doc_text:
                        report_type = "Annual"
                    else:
                        report_type = "Unknown"

                    safe_text = "".join(c for c in doc_text if c not in r'\\/:*?"<>|').strip()

                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", link)
                    human_pause(0.5, 1.2)
                    link.click()
                    print(f"    ⬇️ Download triggered for CSV #{idx + 1} ({safe_text})")
                    human_pause(8, 12)

                    files = [f for f in os.listdir(base_folder) if f.endswith('.zip')]
                    if files:
                        latest_file = max([os.path.join(base_folder, f) for f in files], key=os.path.getctime)
                        dest_folder = os.path.join(base_folder, code, report_type)
                        os.makedirs(dest_folder, exist_ok=True)
                        dest_file = os.path.join(dest_folder, f"{safe_text}.zip")
                        if not os.path.exists(dest_file):
                            os.rename(latest_file, dest_file)
                            print(f"    ✅ Saved as: {dest_file}")
                            zip_queue.put(dest_file)
                            update_code_timestamp(code)
                except Exception as e:
                    print(f"    ⚠️ Failed to download CSV #{idx + 1}: {e}")

        except Exception as e:
            print(f"    ❌ Error for code {code}: {e}")
            mark_code_processed(code, "failed")

        post_result_to_api(code, found)

        if (idx_code + 1) % 10 == 0:
            print("⏳ Taking a longer pause...")
            human_pause(30, 60)
        else:
            human_pause(4, 10)

    print("✅ Scraping complete.")
    driver.quit()
    zip_queue.put(None)

# === Main run ===
if __name__ == "__main__":
    zip_queue = queue.Queue()
    unzip_thread = threading.Thread(target=unzip_worker, args=(zip_queue,))
    unzip_thread.start()
    download_worker(zip_queue)
    unzip_thread.join()

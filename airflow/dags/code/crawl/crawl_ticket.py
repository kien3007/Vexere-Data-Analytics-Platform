# -*- coding: utf-8 -*-
import os
import sys
import csv
import time
import queue
import threading
import traceback
import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# ======================
# CONFIG
# ======================
DEFAULT_WIN_BASE = r"C:\KLTN2\data\raw"
LOCAL_RAW_DIR = os.getenv("LOCAL_RAW_DIR", DEFAULT_WIN_BASE)

WAIT_TIMEOUT = int(os.getenv("CRAWL_WAIT_TIMEOUT", "45"))
CRAWL_RETRY = int(os.getenv("CRAWL_RETRY", "1"))

def initialize_driver():
    options = webdriver.ChromeOptions()
    flags = [
        "--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
        "--disable-gpu", "--disable-extensions",
        "--disable-blink-features=AutomationControlled",
        "--window-size=1920,1080", "--disable-notifications",
        "--disable-popup-blocking", "--lang=vi-VN"
    ]
    for f in flags:
        options.add_argument(f)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(WAIT_TIMEOUT)
    driver.set_script_timeout(WAIT_TIMEOUT)
    return driver

def wait_page_ready(driver):
    WebDriverWait(driver, WAIT_TIMEOUT).until(
        lambda drv: drv.execute_script("return document.readyState") == "complete"
    )

# ======================
# Helpers
# ======================
def scroll_and_click_see_more(driver):
    previous_count = 0
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.0)

        bus_elements = driver.find_elements(By.CLASS_NAME, "bus-name")
        current_count = len(bus_elements)

        if current_count == previous_count:
            try:
                button = WebDriverWait(driver, 2).until(
                    EC.element_to_be_clickable(
                        (By.XPATH,
                         "//button[contains(@class,'load-more') "
                         "or contains(.,'Xem thêm') "
                         "or contains(.,'See more')]")
                    )
                )
                driver.execute_script("arguments[0].click();", button)
                time.sleep(1.0)
                bus_elements = driver.find_elements(By.CLASS_NAME, "bus-name")
                if len(bus_elements) == previous_count:
                    break
                previous_count = len(bus_elements)
                continue
            except Exception:
                break
        previous_count = current_count

def _safe_texts(elems):
    out = []
    for e in elems:
        try:
            out.append(e.text.strip())
        except Exception:
            out.append("")
    return out

# ---- parsers for capacity & available ----
CAPACITY_RE = re.compile(r'(\d+)\s*(?:chỗ|giường|phòng|ghe|giuong)', re.IGNORECASE)
AVAILABLE_RE = re.compile(r'(?:còn|only)\s*(\d+)\s*(?:chỗ|seats?)', re.IGNORECASE)

def parse_capacity(seat_types_text: str):
    if not seat_types_text:
        return None
    nums = CAPACITY_RE.findall(seat_types_text.lower())
    if not nums:
        return None
    try:
        # nếu có nhiều số → lấy số lớn nhất coi như sức chứa
        return int(max(nums, key=lambda x: int(x)))
    except Exception:
        return None

def parse_available(seat_available_text: str):
    if not seat_available_text:
        return None
    m = AVAILABLE_RE.search(seat_available_text.lower())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def extract_bus_data(driver, province):
    bus_names        = _safe_texts(driver.find_elements(By.CLASS_NAME, "bus-name"))
    fares            = _safe_texts(driver.find_elements(By.CLASS_NAME, "fare"))
    departures_time  = _safe_texts(driver.find_elements(By.CSS_SELECTOR, ".from .hour"))
    departures_place = _safe_texts(driver.find_elements(By.CSS_SELECTOR, ".from .place"))
    arrivals_time    = _safe_texts(driver.find_elements(By.CSS_SELECTOR, ".content-to-info .hour"))
    arrivals_place   = _safe_texts(driver.find_elements(By.CSS_SELECTOR, ".content-to-info .place"))
    durations        = _safe_texts(driver.find_elements(By.CLASS_NAME, "duration"))
    seat_types       = _safe_texts(driver.find_elements(By.CLASS_NAME, "seat-type"))
    seat_available   = _safe_texts(driver.find_elements(By.CLASS_NAME, "seat-available"))

    n = min(
        len(bus_names), len(fares), len(departures_time), len(departures_place),
        len(arrivals_time), len(arrivals_place), len(durations),
        len(seat_types), len(seat_available)
    )

    data = []
    date = datetime.now().strftime("%Y-%m-%d")
    bus_id = 1
    for i in range(n):
        route = f"TP.HCM - {province.replace('-', ' ').title()}"

        # ---- compute Ticket_Sold (estimated) ----
        capacity  = parse_capacity(seat_types[i])
        available = parse_available(seat_available[i])
        if capacity is not None and available is not None:
            ticket_sold = max(capacity - available, 0)
        else:
            ticket_sold = ""  # thiếu dữ liệu → để trống

        data.append([
            bus_id,
            bus_names[i],
            date,
            route,
            departures_time[i],
            arrivals_time[i],
            departures_place[i],
            arrivals_place[i],
            durations[i],
            seat_types[i],
            seat_available[i],
            fares[i],
            ticket_sold,              # <-- NEW COLUMN at the end
        ])
        bus_id += 1
    return data

def save_to_csv(data, filename=None):
    if not data:
        print("⚠️ Không có dữ liệu để lưu.")
        return

    date_tag = datetime.now().strftime("%Y-%m-%d")
    folder_path = os.path.join(LOCAL_RAW_DIR, "ticket", f"date={date_tag}")
    os.makedirs(folder_path, exist_ok=True)

    if filename is None:
        filename = f"bus_ticket_{date_tag}.csv"
    filepath = os.path.normpath(os.path.join(folder_path, filename))

    with open(filepath, mode="w", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Bus_Key", "Bus_Name", "Start_Date", "Route",
            "Departure_Time", "Arrival_Time",
            "Departure_Place", "Arrival_Place",
            "Duration", "Seat_Types", "Seat_Available", "Price",
            "Ticket_Sold"  # <-- header for new column
        ])
        writer.writerows(data)

    print(f"💾 Đã lưu tại: {filepath}")

def preload_province(province, key):
    driver = None
    try:
        driver = initialize_driver()
        date = datetime.now().strftime("%d-%m-%Y")
        url = f"https://vexere.com/vi-VN/ve-xe-khach-tu-sai-gon-di-{province}-{key}.html?date={date}"
        print(f"Đang tải trước {province} ...")
        driver.get(url)
        wait_page_ready(driver)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "bus-name"))
        )
        return driver
    except Exception as e:
        print(f"❌ Lỗi preload {province}: {e}")
        traceback.print_exc()
        try:
            if driver:
                driver.quit()
        except Exception:
            pass
        return None

def process_preloaded_province(province, driver):
    if driver is None:
        print(f"Không có driver sẵn cho {province}")
        return []

    try:
        print(f"▶ Đang xử lý {province} ...")
        attempt = 0
        while True:
            try:
                WebDriverWait(driver, 12).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "bus-name"))
                )
                scroll_and_click_see_more(driver)
                break
            except TimeoutException:
                attempt += 1
                if attempt > CRAWL_RETRY:
                    raise
                print(f"↻ Timeout {province}, refresh & thử lại ({attempt}/{CRAWL_RETRY})")
                driver.refresh()
                wait_page_ready(driver)
                time.sleep(1.0)

        data = extract_bus_data(driver, province)
        return data
    except Exception as e:
        print(f"❌ Lỗi xử lý {province}: {e}")
        traceback.print_exc()
        return []
    finally:
        try:
            driver.quit()
        except Exception:
            pass

# ======================
# Main
# ======================
def crwl_ticket():
    provinces_keys = {
        "binh-thuan": "129t1111",
        "binh-dinh": "129t181",
    }

    province_items = list(provinces_keys.items())
    province_queue = queue.Queue()
    for item in province_items:
        province_queue.put(item)

    preloaded_queue = queue.Queue()
    all_bus_data = []
    all_data_lock = threading.Lock()
    task_lock = threading.Lock()

    def preloader_worker():
        while not province_queue.empty():
            try:
                with task_lock:
                    if province_queue.empty():
                        break
                    province, key = province_queue.get()
                drv = preload_province(province, key)
                if drv:
                    preloaded_queue.put((province, drv))
            except Exception as e:
                print(f"⚠️ preloader_worker error: {e}")
                traceback.print_exc()

    def processor_worker():
        while True:
            try:
                try:
                    province, drv = preloaded_queue.get(timeout=1)
                except queue.Empty:
                    if province_queue.empty() and preloaded_queue.empty():
                        break
                    time.sleep(0.5)
                    continue

                data = process_preloaded_province(province, drv)
                with all_data_lock:
                    all_bus_data.extend(data)
            except Exception as e:
                print(f"⚠️ processor_worker error: {e}")
                traceback.print_exc()

    print("Working...")
    preloader_threads, processor_threads = [], []

    for _ in range(1):
        t = threading.Thread(target=preloader_worker, daemon=True)
        t.start()
        preloader_threads.append(t)

    for _ in range(3):
        t = threading.Thread(target=processor_worker, daemon=True)
        t.start()
        processor_threads.append(t)

    for t in preloader_threads:
        t.join()
    for t in processor_threads:
        t.join()

    date_tag = datetime.now().strftime("%Y-%m-%d")
    save_to_csv(all_bus_data, f"bus_ticket_{date_tag}.csv")
    print("✅ Thu thập dữ liệu hoàn tất.")
    return all_bus_data

if __name__ == "__main__":
    crwl_ticket()
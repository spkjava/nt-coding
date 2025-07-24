# === 🛠️ Import Libraries ===

import tkinter as tk
from tkinter import filedialog, scrolledtext
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService
# from webdriver_manager.microsoft import EdgeChromiumDriverManager # ไม่จำเป็นต้องใช้ถ้าผู้ใช้ระบุ path เอง
import time
import os
import shutil
import random
from datetime import datetime

# === 🧾 Credentials และ URLs ===
USERNAME = "csoc_reports"
PASSWORD = "csoc@reports"
LOGIN_URL = "http://nmsgov.ntcsoc.net/Orion/Login.aspx"
REPORT_URL = "http://nmsgov.ntcsoc.net/Orion/reports/viewreports.aspx"

# === 🔢 ตัวแปรนับผลลัพธ์ ===
success_count = 0
fail_count = 0
failed_reports = []

# === 📜 Log ลงในกล่อง GUI ===
def log(text):
    log_box.insert(tk.END, text + "\n")
    log_box.see(tk.END)
    root.update()

# === 📁 เลือกโฟลเดอร์/ไฟล์ผ่าน GUI ===
def browse_path(var, is_file=False):
    if is_file:
        path = filedialog.askopenfilename(
            title="Select msedgedriver.exe",
            filetypes=[("Executable files", "*.exe")]
        )
    else:
        path = filedialog.askdirectory()
    if path:
        var.set(path)

# === ▶️ เริ่มดาวน์โหลด ===
def start_download():
    global success_count, fail_count, failed_reports
    success_count = 0
    fail_count = 0
    failed_reports = []

    download_dir = download_folder.get()
    target_dir = target_folder.get()
    driver_path = msedgedriver_path.get() # ดึงค่า path ของ driver จาก GUI

    # ตรวจสอบว่า path ของ driver ถูกระบุหรือไม่
    if not driver_path or not os.path.exists(driver_path):
        log("❌ Error: Please specify a valid path for 'msedgedriver.exe'.")
        return

    # === 🛠️ ตั้งค่า Edge (Chromium) ===
    edge_options = EdgeOptions()
    edge_options.use_chromium = True
    edge_options.add_argument("--start-maximized")
    prefs = {
        "download.default_directory": download_dir,
        "safeBrowse.enabled": True
    }
    edge_options.add_experimental_option("prefs", prefs)

    try:
        # ใช้ path ที่ผู้ใช้ระบุจาก GUI
        edge_service = EdgeService(driver_path)
        driver = webdriver.Edge(service=edge_service, options=edge_options)
    except Exception as e:
        log(f"❌ Error getting Edge Driver: {e}. Please ensure Edge browser is installed and the correct 'msedgedriver.exe' path is provided.")
        return # หยุดการทำงานถ้าหา Driver ไม่ได้

    driver.implicitly_wait(10)

    try:
        # === 🔐 Login ===
        log("🔐 Logging in...")
        driver.get(LOGIN_URL)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "ctl00_BodyContent_Username")))
        driver.find_element(By.ID, "ctl00_BodyContent_Username").send_keys(USERNAME)
        driver.find_element(By.ID, "ctl00_BodyContent_Password").send_keys(PASSWORD)
        driver.find_element(By.ID, "ctl00_BodyContent_LoginButton").click()
        log("✅ Login successful")

        time.sleep(random.uniform(2, 4))  # หน่วงให้ดูเหมือนมนุษย์

        # === 🔄 Loop หน้า Report ===
        driver.get(REPORT_URL)
        time.sleep(random.uniform(2, 4))

        while True:
            # ค้นหา <a> ที่เป็นรายงาน
            report_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'Report.aspx?ReportID=')]")
            log(f"📄 Found {len(report_links)} reports on this page")

            for i in range(len(report_links)):
                try:
                    # กดเข้าแต่ละ report
                    report_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'Report.aspx?ReportID=')]")
                    name = report_links[i].text.strip()
                    report_links[i].click()
                    log(f"🟢 Opening report: {name}")

                    # คลิก Export to Excel
                    export_btn = WebDriverWait(driver, 20).until(
                        EC.element_to_be_clickable((By.LINK_TEXT, "Export to Excel"))
                    )
                    export_btn.click()
                    log("📥 Export clicked")

                    time.sleep(random.uniform(5, 7))  # รอไฟล์โหลด

                    # ค้นหาไฟล์ล่าสุด และเปลี่ยนชื่อ
                    files = sorted(
                        [os.path.join(download_dir, f) for f in os.listdir(download_dir) if f.endswith(".xlsx")],
                        key=os.path.getctime,
                        reverse=True,
                    )
                    if files:
                        latest = files[0]
                        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
                        new_name = f"{name.replace(' ', '_').replace('/', '-')}_{timestamp}.xlsx"
                        shutil.move(latest, os.path.join(target_dir, new_name))
                        log(f"✅ Saved: {new_name}")
                        success_count += 1
                    else:
                        raise Exception("No file found after export")

                except Exception as e:
                    log(f"❌ Failed: {name} → {e}")
                    failed_reports.append(name)
                    fail_count += 1

                # กลับหน้าหลัก
                driver.get(REPORT_URL)
                time.sleep(random.uniform(2, 4))

            # กด next page ถ้ามี
            try:
                next_btn = driver.find_element(By.LINK_TEXT, "Next")
                next_btn.click()
                time.sleep(random.uniform(2, 4))
            except:
                break

    except Exception as e:
        log(f"❌ Unexpected Error: {e}")

    finally:
        driver.quit()
        log("🚪 Browser closed.")
        log(f"\n📊 Summary:")
        log(f"✅ Success: {success_count}")
        log(f"❌ Failed: {fail_count}")
        if failed_reports:
            log("🛑 Failed Reports:")
            for r in failed_reports:
                log(f" - {r}")

# === 🖥️ สร้าง GUI ด้วย tkinter ===
root = tk.Tk()
root.title("📥 Report Downloader")

# Default folders and driver path
download_folder = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "Downloads")) # Default to user's Downloads
target_folder = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "Desktop", "nt", "report")) # Default to a folder on Desktop
msedgedriver_path = tk.StringVar(value="") # เริ่มต้นเป็นค่าว่าง หรือจะใส่ path default ก็ได้

# Layout
tk.Label(root, text="Download Folder:").pack(padx=5, pady=2, anchor='w')
tk.Entry(root, textvariable=download_folder, width=60).pack(padx=5, pady=2)
tk.Button(root, text="📂 Browse", command=lambda: browse_path(download_folder)).pack(padx=5, pady=2)

tk.Label(root, text="Target Folder:").pack(padx=5, pady=2, anchor='w')
tk.Entry(root, textvariable=target_folder, width=60).pack(padx=5, pady=2)
tk.Button(root, text="📂 Browse", command=lambda: browse_path(target_folder)).pack(padx=5, pady=2)

# New section for msedgedriver.exe path
tk.Label(root, text="msedgedriver.exe Path:").pack(padx=5, pady=2, anchor='w')
tk.Entry(root, textvariable=msedgedriver_path, width=60).pack(padx=5, pady=2)
tk.Button(root, text="🔍 Browse Driver", command=lambda: browse_path(msedgedriver_path, is_file=True)).pack(padx=5, pady=2)

tk.Button(root, text="▶️ Start Download", command=start_download).pack(pady=10)


log_box = scrolledtext.ScrolledText(root, width=80, height=25)
log_box.pack(padx=5, pady=5)

root.mainloop()
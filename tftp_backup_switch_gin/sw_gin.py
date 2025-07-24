import threading
import tkinter as tk
from tkinter import scrolledtext, ttk, filedialog
import telnetlib
import time
import csv
import os
import platform
import subprocess
import concurrent.futures
import webbrowser
from datetime import datetime

# --- CONFIG ---
TELNET_HOST_LIST = """
172.28.130.46
172.28.119.94
172.30.37.102
172.28.108.62
""".strip().splitlines()
TELNET_USER = "tot"
TELNET_PASS = "tot"

SSH_IP_LIST = []

SSH_USER = "tot"
SSH_PASS = "tot"
TFTP_SERVER = "10.223.255.255"  # default, user can overwrite in GUI
# --- OUTPUT SETUP ---
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_folder = "output"
os.makedirs(output_folder, exist_ok=True)
SUMMARY_FILE = os.path.join(output_folder, f"backup_SW_summary_{timestamp}.csv")

# --- GUI Setup ---
root = tk.Tk()
root.title("TFTP Auto Backup GIN Switch Dashboard")
root.geometry("1100x900")

# --- UI ELEMENTS ---
btn_frame = tk.Frame(root)
btn_frame.pack(pady=10)

def open_output_folder():
    webbrowser.open(os.path.abspath(os.path.dirname(SUMMARY_FILE)))

def load_ip_list():
    global SSH_IP_LIST
    file_path = filedialog.askopenfilename(title="Select IP List File", filetypes=[("Text Files", "*.txt")])
    if file_path:
        with open(file_path, 'r') as f:
            SSH_IP_LIST = [line.strip() for line in f if line.strip()]
        ip_status_label.config(text=f"✅ Loaded {len(SSH_IP_LIST)} IPs from file")
    else:
        ip_status_label.config(text="⚠ No file selected")

def check_tftp_server():
    ip = tftp_entry.get().strip()
    reachable = is_pingable(ip)
    if reachable:
        tftp_status_label.config(text=f"🟢 TFTP {ip} is reachable", fg="green")
    else:
        tftp_status_label.config(text=f"🔴 TFTP {ip} unreachable", fg="red")

btn_start = tk.Button(btn_frame, text="▶ Start Backup", font=("Segoe UI", 11), command=lambda: threading.Thread(target=run_backup).start())
btn_start.pack(side=tk.LEFT, padx=5)

btn_export = tk.Button(btn_frame, text="📄 Export Result", font=("Segoe UI", 11), command=open_output_folder)
btn_export.pack(side=tk.LEFT, padx=5)

btn_load_ip = tk.Button(btn_frame, text="📂 Load IP List (txt)", font=("Segoe UI", 11), command=load_ip_list)
btn_load_ip.pack(side=tk.LEFT, padx=5)

ip_status_label = tk.Label(root, text="", font=("Segoe UI", 10), fg="blue")
ip_status_label.pack(pady=2)

# --- TFTP Server Input ---
tftp_frame = tk.Frame(root)
tftp_frame.pack(pady=(0, 5))

tk.Label(tftp_frame, text="TFTP Server:", font=("Segoe UI", 10)).pack(side=tk.LEFT)
tftp_entry = tk.Entry(tftp_frame, font=("Segoe UI", 10), width=30)
tftp_entry.insert(0, TFTP_SERVER)
tftp_entry.pack(side=tk.LEFT, padx=5)

btn_check_tftp = tk.Button(tftp_frame, text="🔍 Check", font=("Segoe UI", 9), command=check_tftp_server)
btn_check_tftp.pack(side=tk.LEFT, padx=5)

tftp_status_label = tk.Label(root, text="", font=("Segoe UI", 10))
tftp_status_label.pack()

summary_box = tk.LabelFrame(root, text="📊 Summary", padx=10, pady=5)
summary_box.pack(fill="x", padx=10)


summary_text = tk.Label(summary_box, justify=tk.LEFT, anchor="w", font=("Segoe UI", 10))
summary_text.pack(fill="x")
time_label = tk.Label(summary_box, text="⏱ Elapsed Time: 00:00", font=("Segoe UI", 10))
time_label.pack(side=tk.RIGHT, padx=10)

result_frame = tk.LabelFrame(root, text="📋 Device Backup Results")
result_frame.pack(fill="both", expand=True, padx=10, pady=10)

columns = ("IP", "Ping", "Status", "Error", "Hostname")
tree = ttk.Treeview(result_frame, columns=columns, show="headings")
for col in columns:
    tree.heading(col, text=col, anchor="center")
    tree.column(col, width=200 if col != "Error" else 300, anchor="center")
tree.pack(fill="both", expand=True, anchor="center")

shell_box = scrolledtext.ScrolledText(root, wrap=tk.WORD, height=10, font=("Consolas", 9), bg="#111", fg="#0f0")
shell_box.pack(fill="x", padx=10, pady=(0,10))
shell_box.insert(tk.END, "===== Telnet/SSH Raw Shell Output =====\n")
shell_box.config(state=tk.DISABLED)

# --- Functions ---

def is_pingable(ip):
    param = "-n" if platform.system().lower() == "windows" else "-c"
    # ✅ ปิดทั้ง stdout และ stderr ไม่ให้เปิด console หรือโผล่ข้อความ
    with open(os.devnull, 'w') as DEVNULL:
        result = subprocess.call(["ping", param, "1", ip],
                                 stdout=DEVNULL,
                                 stderr=DEVNULL,
                                 creationflags=subprocess.CREATE_NO_WINDOW if platform.system().lower() == "windows" else 0)
    return result == 0
def log_output(text):
    shell_box.config(state=tk.NORMAL)
    shell_box.insert(tk.END, text + "\n")
    shell_box.see(tk.END)
    shell_box.config(state=tk.DISABLED)
    root.update_idletasks()

def connect_and_backup_via_telnet(ip):
    try:
        tftp_ip = tftp_entry.get().strip()
        filename = f"Backup-Sw_Gin-{ip}"
        full_cmd = f"copy running-config tftp://{tftp_ip}/{filename}"

        log_output(f"\n[{ip}] 🚀 Starting Telnet Session...")
        tn = telnetlib.Telnet(ip, timeout=5)

        log_output(f"[{ip}] 🔍 Expecting 'username:'")
        received = tn.read_until(b"username:", timeout=5)
        log_output(f"[{ip}] ⏬ Received: {received.decode(errors='ignore').strip()}")
        tn.write(b"tot\n")
        log_output(f"[{ip}] ⏫ Sent: tot")

        log_output(f"[{ip}] 🔍 Expecting 'Password:'")
        received = tn.read_until(b"Password:", timeout=5)
        log_output(f"[{ip}] ⏬ Received: {received.decode(errors='ignore').strip()}")
        tn.write(b"tot\n")
        log_output(f"[{ip}] ⏫ Sent: tot")

        log_output(f"[{ip}] 🔍 Waiting for prompt '#' after login...")
        received = tn.read_until(b"#", timeout=10)
        log_output(f"[{ip}] ⏬ Received prompt: {received.decode(errors='ignore').strip()}")

        # ส่งคำสั่ง backup
        log_output(f"[{ip}] 📤 Sending backup command: {full_cmd}")
        tn.write(full_cmd.encode("ascii") + b"\n")
        log_output(f"[{ip}] ⏫ Sent: {full_cmd}")

        # รอ prompt "Destination filename"
        #received = tn.read_until(b"Destination filename", timeout=10)
        #log_output(f"[{ip}] ⏬ Received: {received.decode(errors='ignore').strip()}")

        #tn.write(b"\n")
        #log_output(f"[{ip}] ⏫ Sent: [Enter] to confirm filename")

        # รอจนเสร็จหรือ timeout
        log_output(f"[{ip}] ⏳ Waiting for completion...")
        output = b""
        start = time.time()

        while True:
            try:
                chunk = tn.read_very_eager()
                if chunk:
                    output += chunk
                    decoded = output.decode("utf-8", errors="ignore")

                    # Live log
                    log_output(f"[{ip}] 📦 Chunk:\n{chunk.decode('utf-8', errors='ignore').strip()}")

                    if decoded.strip().endswith("#") or decoded.strip().endswith(">"):  # รองรับ prompt
                        break

                if time.time() - start > 30:
                    break

                time.sleep(0.3)
            except EOFError:
                break

        final_output = output.decode("utf-8", errors="ignore")
        log_output(f"[{ip}] 📦 Output (full):\n{final_output.strip()}")

        # ✅ เพิ่มคำที่แปลว่า "Upload สำเร็จ" ให้รองรับหลาย vendor
        success_indicators = [
            "copied",
            "copy operation was completed successfully",
            "%copy-n-trap",
            "copy: ",
            "uploading done",
            "upload complete",
            "file successfully transferred",
            "bytes copied",
            "transfer complete",
            "transfer ok",
            "file transfer completed",
            "tftp upload success"
        ]

        if any(indicator in final_output.lower() for indicator in success_indicators):
            log_output(f"[{ip}] ✅ Backup SUCCESS")
            return "SUCCESS", "", filename
        else:
            log_output(f"[{ip}] ❌ Backup FAILED – No known success indicator found")
            return "FAILED", "No known success indicator", filename



    except Exception as e:
        log_output(f"[{ip}] ❌ ERROR: {e}")
        return "FAILED", str(e), ""




def export_results(results, success_count, skip_count, online_count):
    try:
        with open(SUMMARY_FILE, "w", newline='', encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["IP Address", "Ping Status", "Backup Status", "Error Detail", "SSH Hostname"])
            for row in results:
                writer.writerow(row)
            writer.writerow([])
            writer.writerow(["📋 Summary"])
            writer.writerow(["Total Devices", len(results)])
            writer.writerow(["🟢 Online Devices", online_count])
            writer.writerow(["✅ Backup Success", success_count])
            writer.writerow(["❌ Backup Failed", len(results) - skip_count - success_count])
            writer.writerow(["⏭️ Skipped Offline", skip_count])
            log_output(f"\n📄 Exported summary to: {os.path.abspath(SUMMARY_FILE)}")
    except Exception as e:
        log_output(f"[ERROR] Export failed: {e}")

def update_time_monitor(start_time):
    def update():
        if not update_time_monitor.running:
            return
        elapsed = int(time.time() - start_time)
        mins, secs = divmod(elapsed, 60)
        time_label.config(text=f"⏱ Elapsed Time: {mins:02}:{secs:02}")
        root.after(1000, update)

    update_time_monitor.running = True
    update()



def run_backup():
    if not SSH_IP_LIST:
        ip_status_label.config(text="❌ No IPs loaded. Please load a list first.", fg="red")
        return
    start_time = time.time()
    update_time_monitor(start_time)
    btn_start.config(state=tk.DISABLED)
    results = []
    for row in tree.get_children():
        tree.delete(row)

    # ✅ เพิ่มตรงนี้
    success_count = 0
    skip_count = 0
    online_count = 0

    def task(ip):
        nonlocal success_count, skip_count, online_count  # ✅ เพื่อให้ใช้ตัวแปรด้านนอกได้
        if is_pingable(ip):
            online_count += 1
            status, error, hostname = connect_and_backup_via_telnet(ip)
            if status == "SUCCESS":
                success_count += 1
            result = (ip, "Online", status, error, hostname)
        else:
            skip_count += 1
            result = (ip, "Offline", "SKIPPED", "Host unreachable", "")
        results.append(result)
        tree.insert("", tk.END, values=result)

        # 🔁 อัปเดต summary แบบ real-time
        summary_text.config(text=(
            f"💻 Total Devices: {len(SSH_IP_LIST)}    "
            f"🟢 Online: {online_count}    "
            f"⏭️ Offline / Skip: {skip_count}     "
            f"✅ Success: {success_count}    "
            f"❌ Failed: {online_count - success_count}    "
        ))

    # รันพร้อมกันหลาย task
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(task, ip) for ip in SSH_IP_LIST]
        concurrent.futures.wait(futures)
    
    # export สรุปเมื่อเสร็จทุก IP
    export_results(results, success_count, skip_count, online_count)
    btn_start.config(state=tk.NORMAL)
    update_time_monitor.running = False

def run_restore():
    ip = restore_ip_entry.get().strip()
    config_file = restore_file_path.get().strip()
    tftp_ip = tftp_entry.get().strip()

    if not ip or not config_file:
        log_output("❌ Please fill in both IP and config file.")
        return

    if not os.path.exists(config_file):
        log_output("❌ Config file not found.")
        return

    config_filename = os.path.basename(config_file)

    log_output("=== 🚀 Starting Restore Process ===")
    log_output(f"🖥 Target Device IP: {ip}")
    log_output(f"📁 Selected Config File: {config_filename}")
    log_output(f"🌐 TFTP Server: {tftp_ip}")

    try:
        log_output(f"🔌 Connecting via Telnet to {ip}...")
        tn = telnetlib.Telnet(ip, timeout=5)

        log_output("🔐 Logging in...")
        tn.read_until(b"username:", timeout=5)
        tn.write(TELNET_USER.encode("ascii") + b"\n")
        tn.read_until(b"Password:", timeout=5)
        tn.write(TELNET_PASS.encode("ascii") + b"\n")
        tn.read_until(b"#", timeout=10)
        log_output("✅ Telnet Login Success")

        # --- Step 1: copy tftp to running-config
        log_output("📤 Sending command: copy tftp://<ip>/<file> running-config")
        copy_cmd = f"copy tftp://{tftp_ip}/{config_filename} running-config\n"
        tn.write(copy_cmd.encode("ascii"))
        time.sleep(1)

        log_output("⏳ Waiting for copy to complete...")
        output = tn.read_until(b"#", timeout=30).decode("utf-8", errors="ignore")
        log_output(f"📦 Output:\n{output.strip()}")

        if "copied" not in output.lower():
            log_output("⚠️ Warning: Config may not have been copied correctly")
        
        # --- Step 2: copy running-config startup-config
        log_output("📤 Saving config to startup-config...")
        tn.write(b"copy running-config startup-config\n")
        time.sleep(1)

        tn.read_until(b"[startup-config]", timeout=5)
        tn.write(b"Y\n")

        save_output = tn.read_until(b"#", timeout=10).decode("utf-8", errors="ignore")
        log_output(f"📦 Output:\n{save_output.strip()}")

        # --- Step 3: (optional) write
        log_output("📤 Sending 'write' command...")
        tn.write(b"write\n")
        tn.read_until(b"[startup-config]", timeout=5)
        tn.write(b"Y\n")
        tn.read_until(b"#", timeout=5)

        log_output(f"✅ Restore and Save COMPLETE for {ip}")

    except Exception as e:
        log_output(f"❌ ERROR during Restore to {ip}: {e}")

    log_output("===  Restore Process ===\n")



def ping_restore_device():
    ip = restore_ip_entry.get().strip()
    if not ip:
        log_output("⚠ Please enter an IP to ping.")
        return
    if is_pingable(ip):
        log_output(f"🟢 {ip} is reachable (Ping OK)")
    else:
        log_output(f"🔴 {ip} is unreachable (Ping FAIL)")




# --- MODERN OCEAN THEME CONFIG ---
modern_bg = "#20232A"          # พื้นหลังหลัก
modern_fg = "#E8F1F2"          # ตัวอักษรหลัก
accent_color = "#0BA5EC"       # สีฟ้าสว่าง
entry_bg = "#2A2D37"           # ช่องกรอก
tree_bg = "#1F2229"            # ตาราง
tree_fg = "#E8F1F2"            # ตัวอักษรตาราง
highlight_color = "#3FB950"    # สีเน้นเขียว

# ตั้งค่าพื้นหลังของ root
root.configure(bg=modern_bg)

# ปรับพื้นหลัง + ตัวอักษรทุก widget (Frame, LabelFrame, Label)
for widget in root.winfo_children():
    if isinstance(widget, (tk.Frame, tk.LabelFrame)):
        widget.configure(bg=modern_bg)
    elif isinstance(widget, tk.Label):
        widget.configure(bg=modern_bg, fg=modern_fg)


# ปรับเฉพาะ label ที่ไม่เข้ากลุ่มข้างบน
ip_status_label.config(bg=modern_bg, fg=accent_color)
tftp_status_label.config(bg=modern_bg, fg=accent_color)
summary_text.config(bg=modern_bg, fg=modern_fg)
summary_box.config(bg=modern_bg, fg=modern_fg)
result_frame.config(bg=modern_bg, fg=modern_fg)
time_label.config(fg=accent_color, bg=modern_bg)

# ปรับ scrolledtext shell box
shell_box.config(bg="#141421", fg="#80cbc4", insertbackground="white")

# ปรับปุ่มให้ดูเรียบ modern
button_list = [btn_start, btn_export, btn_load_ip, btn_check_tftp]
for btn in button_list:
    btn.config(bg="#333344", fg=modern_fg, activebackground="#444455", activeforeground=accent_color, relief=tk.FLAT)

# Entry สี
tftp_entry.config(bg=entry_bg, fg=modern_fg, insertbackground=modern_fg)

# Treeview modern style
style = ttk.Style()
style.theme_use("default")
style.configure("Treeview",
                background=tree_bg,
                foreground=tree_fg,
                fieldbackground=tree_bg,
                rowheight=24,
                bordercolor=modern_bg,
                borderwidth=0)
style.configure("Treeview.Heading",
                background="#333344",
                foreground=accent_color,
                font=("Segoe UI", 10, "bold"))
style.map("Treeview",
          background=[("selected", highlight_color)],
          foreground=[("selected", "white")])
# --- Restore Section ---
restore_frame = tk.LabelFrame(root, text="🛠 Restore Config", padx=10, pady=5)
restore_frame.pack(fill="x", padx=10, pady=(5, 10))

tk.Label(restore_frame, text="Restore IP:", font=("Segoe UI", 10), bg=modern_bg, fg=modern_fg).pack(side=tk.LEFT)
restore_ip_entry = tk.Entry(restore_frame, font=("Segoe UI", 10), width=20, bg=entry_bg, fg=modern_fg, insertbackground=modern_fg)
restore_ip_entry.pack(side=tk.LEFT, padx=5)

tk.Label(restore_frame, text="Config File:", font=("Segoe UI", 10), bg=modern_bg, fg=modern_fg).pack(side=tk.LEFT)
restore_file_path = tk.StringVar()
restore_file_entry = tk.Entry(restore_frame, textvariable=restore_file_path, font=("Segoe UI", 10), width=40, bg=entry_bg, fg=modern_fg, insertbackground=modern_fg)
restore_file_entry.pack(side=tk.LEFT, padx=5)

def browse_config_file():
    file_path = filedialog.askopenfilename(title="Select Config File", filetypes=[("All Files", "*.*")])
    if file_path:
        restore_file_path.set(file_path)

btn_browse_restore = tk.Button(restore_frame, text="📂 Browse", font=("Segoe UI", 9), command=browse_config_file)
btn_browse_restore.pack(side=tk.LEFT, padx=5)

btn_restore = tk.Button(restore_frame, text="📥 Start Restore", font=("Segoe UI", 10),
                        command=lambda: threading.Thread(target=run_restore).start())
btn_restore.pack(side=tk.LEFT, padx=5)
btn_ping_restore = tk.Button(restore_frame, text="🔍 Ping Device", font=("Segoe UI", 9),
                             command=lambda: ping_restore_device())
btn_ping_restore.pack(side=tk.LEFT, padx=5)

try:
    root.mainloop()
except Exception as e:
    with open("gui_error.log", "w") as f:
        f.write(str(e))




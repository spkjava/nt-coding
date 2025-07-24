import requests
import re
import html
import json
import xml.etree.ElementTree as ET
from ftfy import fix_text
import io
import csv
import datetime
import os
import argparse
import pandas as pd
from flask import Flask, request, render_template, jsonify, send_from_directory
import tempfile
import threading
import uuid
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import logging
from queue import Queue
import zipfile
import shutil # สำหรับลบ directory
import time # สำหรับ threading.Timer ในการ cleanup

app = Flask(__name__)

# --- สถานะการประมวลผลและ Lock สำหรับ Thread-safe ---
processing_status = {}
status_lock = threading.Lock()

# --- ตั้งค่า Logger และ Log Queue ---
log_queue = Queue() # สร้าง Queue สำหรับเก็บ log

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # ตั้งค่าระดับ log ที่จะบันทึก

# ลบ handler เก่าออกก่อนเพื่อป้องกันการเพิ่มซ้ำเมื่อ reload (สำหรับ Flask dev server)
if logger.handlers:
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

class QueueHandler(logging.Handler):
    """
    Handler ที่จะส่ง log record ไปยัง Queue
    """
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        try:
            # ONLY send the formatted message to the queue, without full log details
            # This is where we control what goes to the frontend
            msg = self.format(record)
            
            # --- IMPORTANT: Custom formatting for frontend logs ---
            # Remove timestamp, level, and job ID from logs sent to frontend for clarity
            # Example: "2025-07-17 08:54:55,123 - INFO - Job f17846f3-4d62-44d9-a0a1-0176c18acd5c: กำลังประมวลผล NodeID: 185271..."
            # We want just: "กำลังประมวลผล NodeID: 185271..."
            log_pattern = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - (INFO|WARNING|ERROR|CRITICAL) - (Job [0-9a-f-]+: )?(.*)")
            match = log_pattern.match(msg)
            if match:
                clean_msg = match.group(3) # Get the message part
                self.queue.put(clean_msg)
            else:
                self.queue.put(msg) # Fallback if pattern doesn't match
            # --- End Custom formatting ---

        except Exception:
            self.handleError(record)

# Console Handler (แสดง log ใน Terminal) - เก็บ format เต็มไว้สำหรับ debugging
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# Queue Handler (ส่ง log ไปยัง Queue สำหรับ Frontend)
queue_handler = QueueHandler(log_queue)
# ไม่ต้องตั้ง formatter ที่นี่ เพราะเราจะ format เองใน emit()
logger.addHandler(queue_handler)

# --- ตั้งค่าฟอนต์ภาษาไทยสำหรับ PDF ---
THAI_FONT_NAME = 'THSarabunNew'
# ตรวจสอบให้แน่ใจว่า 'THSarabunNew.ttf' อยู่ใน directory เดียวกันกับ app.py
THAI_FONT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'THSarabunNew.ttf')

THAI_FONT_REGISTERED = False
if os.path.exists(THAI_FONT_PATH):
    try:
        pdfmetrics.registerFont(TTFont(THAI_FONT_NAME, THAI_FONT_PATH))
        THAI_FONT_REGISTERED = True
        logger.info(f"Thai font '{THAI_FONT_NAME}' registered successfully from '{THAI_FONT_PATH}'.")
    except Exception as e:
        logger.error(f"ERROR: Could not register Thai font '{THAI_FONT_NAME}'. Error: {e}")
else:
    logger.warning(f"WARNING: Thai font file '{THAI_FONT_PATH}' not found. Please ensure the font file is in the same directory as the script.")

# --- ฟังก์ชันสำหรับประมวลผลข้อมูล ---
def get_data_from_api(nod_id, itf_id, job_id):
    """ดึงข้อมูลจาก API และแปลงเป็น JSON"""
    url = "http://1.179.233.116:8082/api_csoc_02/server_solarwinds_gin.php"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://1.179.233.116/api_csoc_02/server_solarwinds_gin.php/circuitStatus"
    }
    body = f"""<?xml version="1.1" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <circuitStatus xmlns="http://1.179.233.116/soap/#Service_Solarwinds_gin">
      <nodID>{nod_id}</nodID>
      <itfID>{itf_id}</itfID>
    </circuitStatus>
  </soap:Body>
</soap:Envelope>"""

    try:
        resp = requests.post(url, data=body, headers=headers, timeout=10)
        resp.raise_for_status()
        match = re.search(r"(<\?xml.*?</SOAP-ENV:Envelope>)", resp.text, re.DOTALL)
        if not match:
            logger.warning(f"ไม่พบ XML Response สำหรับ NodeID: {nod_id}, Interface ID: {itf_id}")
            return None
        
        root = ET.fromstring(match.group(1))
        return_tag = root.find(".//{*}return")
        if return_tag is None or not return_tag.text:
            logger.warning(f"API ไม่มีข้อมูลตอบกลับสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}")
            return None

        raw_text = return_tag.text
        html_unescaped = html.unescape(raw_text)
        fixed_text = fix_text(bytes(html_unescaped, "utf-8").decode("unicode_escape"))
        parsed_json = json.loads(fixed_text)
        return parsed_json
    except requests.exceptions.RequestException as req_e:
        logger.error(f"❌ ดึงข้อมูล NodeID: {nod_id}, Interface ID: {itf_id} ล้มเหลว: {req_e}")
        return None
    except ET.ParseError as parse_e:
        logger.error(f"❌ XML Parsing ผิดพลาดสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}: {parse_e}")
        return None
    except json.JSONDecodeError as json_e:
        logger.error(f"❌ JSON Decoding ผิดพลาดสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}: {json_e}")
        return None
    except Exception as e:
        logger.error(f"❌ ข้อผิดพลาดไม่คาดคิดสำหรับ NodeID: {nod_id}, Interface ID: {itf_id}: {e}")
        return None

def process_json_data(raw_json_data, job_id):
    """ประมวลผลข้อมูล JSON เพื่อให้พร้อมสำหรับสร้างไฟล์"""
    column_mapping = {
        "รหัสหน่วยงาน": "Customer_Curcuit_ID",
        "ชื่อหน่วยงาน": "Address",
        "วันที่และเวลา": "Timestamp",
        "ขนาดBandwidth (หน่วย Mbps)": "Bandwidth",
        "ปริมาณการใช้งาน incoming (หน่วย bps)": "In_Averagebps",
        "ปริมาณการใช้งาน outcoming (หน่วย bps)": "Out_Averagebps"
    }
    desired_headers_th = list(column_mapping.keys())
    
    # ตรวจสอบว่า raw_json_data เป็น list หรือ dict
    data_to_process = raw_json_data if isinstance(raw_json_data, list) else [raw_json_data]

    # --- Step 1: Parse all existing timestamps and find earliest/latest dates ---
    formatted_data = []
    earliest_json_date = None
    latest_json_date = None

    for item in data_to_process:
        date_time_value = item.get("Timestamp")
        if isinstance(date_time_value, dict) and 'date' in date_time_value:
            try:
                # Parse timestamp from JSON string. Keep original format to reconstruct for output
                dt_obj = datetime.datetime.strptime(date_time_value['date'], '%Y-%m-%d %H:%M:%S.%f')
                formatted_item = item.copy()
                formatted_item['Parsed_Timestamp'] = dt_obj
                formatted_data.append(formatted_item)

                if earliest_json_date is None or dt_obj < earliest_json_date:
                    earliest_json_date = dt_obj
                if latest_json_date is None or dt_obj > latest_json_date:
                    latest_json_date = dt_obj
            except ValueError:
                # If date parsing fails, just add the item without a parsed timestamp
                # These items won't be considered for filling gaps
                formatted_data.append(item.copy()) 
                logger.warning(f"⚠️ ไม่สามารถ parse วันที่ได้: {date_time_value.get('date')}. รายการนี้จะถูกข้ามการเติมข้อมูล.")

        else:
            # If 'Timestamp' is not a dict or 'date' is missing, add as is
            formatted_data.append(item.copy())

    # If no valid dates were found, just process the raw data as is
    if earliest_json_date is None or latest_json_date is None:
        logger.warning(f"ไม่พบข้อมูลวันที่ที่ถูกต้องใน JSON สำหรับการเติมวันที่/ชั่วโมงที่ขาดหายไป")
        processed_data = []
        for item in data_to_process: # Use original data_to_process to ensure all items are included
            row_data = {}
            for th_header, json_key in column_mapping.items():
                value = item.get(json_key, '')
                if th_header in ["ปริมาณการใช้งาน incoming (หน่วย bps)", "ปริมาณการใช้งาน outcoming (หน่วย bps)"]:
                    try:
                        value_float = float(value)
                        row_data[th_header] = f"{int(value_float):,}" # Format with comma
                    except (ValueError, TypeError):
                        row_data[th_header] = str(value)
                elif th_header == "วันที่และเวลา" and isinstance(value, dict) and 'date' in value:
                    try:
                        dt_obj = datetime.datetime.strptime(value['date'], '%Y-%m-%d %H:%M:%S.%f')
                        row_data[th_header] = dt_obj.strftime('%Y-%m-%d %H.%M.%S')
                    except ValueError:
                        row_data[th_header] = str(value)
                elif th_header == "ขนาดBandwidth (หน่วย Mbps)":
                    if "FTTx" in str(value):
                        row_data[th_header] = "20 Mbps." # Changed from 0 Mbps.
                    else:
                        try:
                            numeric_value = float(re.search(r'[\d.]+', str(value)).group())
                            row_data[th_header] = f"{int(numeric_value):,} Mbps." # Format with comma
                        except (ValueError, TypeError, AttributeError):
                            row_data[th_header] = str(value)
                else:
                    row_data[th_header] = str(value)
            processed_data.append(row_data)
        return desired_headers_th, processed_data, {} # Return empty averages

    # --- Step 2: Determine the full time range for filling ---
    first_day_of_month_start_hour = earliest_json_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # The end point for filling is the last hour of the latest_json_date
    # We want to fill up to and including the last full hour of the latest data.
    # If latest_json_date is 2025-07-13 10:30:00, we want to fill up to 2025-07-13 23:00:00
    last_day_of_data_end_hour = latest_json_date.replace(hour=23, minute=0, second=0, microsecond=0)


    # --- Step 3: Get common data for new entries (customer ID, name, bandwidth) ---
    first_actual_entry = None
    for item in formatted_data:
        if 'Parsed_Timestamp' in item: # Find the first entry that actually has a parsed timestamp
            first_actual_entry = item
            break
    
    # Use default empty strings if no valid first entry is found
    customer_id = first_actual_entry.get("Customer_Curcuit_ID", "") if first_actual_entry else ""
    customer_name = first_actual_entry.get("Address", "") if first_actual_entry else ""
    bandwidth = first_actual_entry.get("Bandwidth", "") if first_actual_entry else ""

    # --- Step 4: Create a set of existing date-hour combinations for quick lookup ---
    existing_date_hours = set()
    for item in formatted_data:
        if 'Parsed_Timestamp' in item:
            existing_date_hours.add(item['Parsed_Timestamp'].replace(minute=0, second=0, microsecond=0)) # Store only year, month, day, hour

    # --- Step 5: Generate and fill in missing entries ---
    dates_to_add_data = []
    current_hour_dt = first_day_of_month_start_hour

    # Loop from the first hour of the month to the last hour of the latest data day
    while current_hour_dt <= last_day_of_data_end_hour:
        if current_hour_dt not in existing_date_hours:
            # Create a new entry for the missing hour
            missing_entry = {
                "Customer_Curcuit_ID": customer_id,
                "Address": customer_name,
                "Timestamp": {"date": current_hour_dt.strftime('%Y-%m-%d %H:%M:%S.%f')},
                "Bandwidth": bandwidth,
                "In_Averagebps": "0",
                "Out_Averagebps": "0",
                "Parsed_Timestamp": current_hour_dt
            }
            dates_to_add_data.append(missing_entry)
            logger.info(f"✨ เพิ่มข้อมูลสำหรับชั่วโมงที่ขาดหายไป: {current_hour_dt.strftime('%Y-%m-%d %H:%M')}")
        current_hour_dt += datetime.timedelta(hours=1) # Move to the next hour
    
    # --- Step 6: Combine all data and sort by timestamp ---
    combined_data = dates_to_add_data + formatted_data
    
    # Ensure all data is sorted by the parsed timestamp
    # Filter out items that might not have a Parsed_Timestamp due to parsing errors
    sorted_combined_data = sorted(
        [item for item in combined_data if 'Parsed_Timestamp' in item],
        key=lambda x: x['Parsed_Timestamp']
    )

    # --- Step 7: Calculate monthly averages for In_Averagebps and Out_Averagebps ---
    total_sum_in = 0
    total_count_in = 0
    total_sum_out = 0
    total_count_out = 0

    for item in sorted_combined_data:
        try:
            in_bps = float(item.get("In_Averagebps", 0))
            total_sum_in += in_bps
            total_count_in += 1
        except (ValueError, TypeError):
            pass # Ignore non-numeric values

        try:
            out_bps = float(item.get("Out_Averagebps", 0))
            total_sum_out += out_bps
            total_count_out += 1
        except (ValueError, TypeError):
            pass # Ignore non-numeric values
    
    monthly_averages = {}
    if total_count_in > 0 and total_count_out > 0:
        avg_in_month = (total_sum_in / total_count_in)
        avg_out_month = (total_sum_out / total_count_out)
        monthly_averages = {
            "avg_in_month": int(avg_in_month),
            "avg_out_month": int(avg_out_month)
        }

    # --- Step 8: Final formatting for output ---
    processed_data = []
    for item in sorted_combined_data:
        row_data = {}
        for th_header, json_key in column_mapping.items():
            # For 'วันที่และเวลา', use the 'Parsed_Timestamp' for consistent formatting
            if th_header == "วันที่และเวลา":
                row_data[th_header] = item['Parsed_Timestamp'].strftime('%Y-%m-%d %H.%M.%S')
            else:
                value = item.get(json_key, '')
                if th_header in ["ปริมาณการใช้งาน incoming (หน่วย bps)", "ปริมาณการใช้งาน outcoming (หน่วย bps)"]:
                    try:
                        value_float = float(value)
                        row_data[th_header] = f"{int(value_float):,}" # Format with comma
                    except (ValueError, TypeError):
                        row_data[th_header] = str(value)
                elif th_header == "ขนาดBandwidth (หน่วย Mbps)":
                    if "FTTx" in str(value):
                        row_data[th_header] = "20 Mbps." # Changed from 0 Mbps.
                    else:
                        try:
                            numeric_value = float(re.search(r'[\d.]+', str(value)).group())
                            row_data[th_header] = f"{int(numeric_value):,} Mbps." # Format with comma
                        except (ValueError, TypeError, AttributeError):
                            row_data[th_header] = str(value)
                else:
                    row_data[th_header] = str(value)
        processed_data.append(row_data)

    return desired_headers_th, processed_data, monthly_averages

def export_to_csv(headers, data, monthly_averages, filename, job_id, node_name):
    """สร้างและบันทึกไฟล์ CSV โดยให้ 'รหัสหน่วยงาน' และ 'ชื่อหน่วยงาน' แสดงในทุกแถว
       และเพิ่มแถวสำหรับค่าเฉลี่ยรวมทั้งเดือนในแถวสุดท้าย
       
       แก้ไข:
       - 'ปริมาณการใช้งาน incoming (หน่วย bps)' เป็น 'In_Averagebps'
       - 'ปริมาณการใช้งาน outcoming (หน่วย bps)' เป็น 'Out_Averagebps'
    """
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            cw = csv.writer(f)
            if headers and data:
                # กำหนดหัวตารางใหม่ตามที่ต้องการแสดงใน CSV
                csv_display_headers = [
                    "รหัสหน่วยงาน",
                    "ชื่อหน่วยงาน",
                    "วันที่และเวลา",
                    "ขนาดBandwidth (หน่วย Mbps)",
                    "In_Averagebps",  # เปลี่ยนชื่อหัวตาราง
                    "Out_Averagebps"  # เปลี่ยนชื่อหัวตาราง
                ]
                cw.writerow(csv_display_headers) # เขียนหัวตารางใหม่ลงไป
                
                for row in data:
                    new_row = [
                        row.get('รหัสหน่วยงาน', ''),
                        row.get('ชื่อหน่วยงาน', ''),
                        row.get('วันที่และเวลา', ''),
                        row.get('ขนาดBandwidth (หน่วย Mbps)', ''),
                        # ดึงข้อมูลจากคีย์เดิมที่เป็นภาษาไทย ซึ่งเป็นคีย์ที่อยู่ใน 'data' ที่ถูกส่งเข้ามา
                        row.get('ปริมาณการใช้งาน incoming (หน่วย bps)', ''),
                        row.get('ปริมาณการใช้งาน outcoming (หน่วย bps)', '')
                    ]
                    cw.writerow(new_row)
                
                # Insert monthly average row at the very end
                if monthly_averages:
                    avg_in = monthly_averages['avg_in_month']
                    avg_out = monthly_averages['avg_out_month']
                    cw.writerow([
                        '', '', # Empty for customer ID/name
                        'Total', 
                        '', # Bandwidth
                        f'{avg_in:,}', 
                        f'{avg_out:,}'
                    ])
            else:
                cw.writerow(["No Data"])
        logger.info(f"✅ สร้าง CSV สำหรับ '{node_name}' สำเร็จแล้ว")
        return True, "Success"
    except Exception as e:
        logger.error(f"❌ สร้าง CSV สำหรับ '{node_name}' ล้มเหลว: {e}")
        return False, str(e)


def export_to_pdf(headers, data, monthly_averages, filename, job_id, node_name):
    """สร้างและบันทึกไฟล์ PDF โดยให้แต่ละวันขึ้นหน้าใหม่ และเพิ่มค่าเฉลี่ยรวมทั้งเดือนในแถวสุดท้ายของตารางข้อมูลสุดท้าย"""
    try:
        doc = SimpleDocTemplate(filename, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []

        # Constants for page dimensions
        # letter size is 8.5 x 11 inches. 1 inch = 72 points
        page_width, page_height = letter
        left_margin = right_margin = 0.5 * inch # Set a consistent margin

        # Calculate available width for the table
        available_width = page_width - (left_margin + right_margin)

        if headers and data:
            data_by_date = {}
            for row in data:
                date_time_str = row.get('วันที่และเวลา', '')
                try:
                    date_key = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H.%M.%S').strftime('%Y-%m-%d')
                except ValueError:
                    date_key = 'Uncategorized' # Fallback for malformed date
                if date_key not in data_by_date:
                    data_by_date[date_key] = []
                data_by_date[date_key].append(row)
            
            sorted_date_keys = sorted(data_by_date.keys())
            last_date_key = sorted_date_keys[-1] if sorted_date_keys else None

            first_page = True
            for i, date_key in enumerate(sorted_date_keys):
                group_data = data_by_date[date_key]

                if not first_page:
                    elements.append(PageBreak())
                
                # Title
                title_style = styles['Title']
                if THAI_FONT_REGISTERED:
                    title_style.fontName = THAI_FONT_NAME
                title_style.fontSize = 18
                title_style.alignment = 1
                elements.append(Paragraph("Custumer Interface Summary Report by Hour", title_style))
                elements.append(Spacer(1, 0.2 * inch))

                # Subtitle (Date)
                sub_title_style = ParagraphStyle('SubTitle', parent=styles['Normal'])
                if THAI_FONT_REGISTERED:
                    sub_title_style.fontName = THAI_FONT_NAME
                sub_title_style.fontSize = 12
                sub_title_style.alignment = 1 
                elements.append(Paragraph(f"<b>รายงานประจำวันที่:</b> {date_key}", sub_title_style))
                elements.append(Spacer(1, 0.2 * inch))

                table_headers = [
                    "รหัสหน่วยงาน",
                    "ชื่อหน่วยงาน",
                    "วันที่และเวลา",
                    "ขนาดBandwidth \n(หน่วย Mbps)",
                    "ปริมาณการใช้งาน incoming\n (หน่วย bps)",
                    "ปริมาณการใช้งาน outcoming\n (หน่วย bps)"
                ]
                
                table_data = [table_headers]
                last_customer_id = None
                last_customer_name = None
                
                for row in group_data:
                    current_customer_id = row.get('รหัสหน่วยงาน', '')
                    current_customer_name = row.get('ชื่อหน่วยงาน', '')
                    display_customer_id = current_customer_id if current_customer_id != last_customer_id else ''
                    display_customer_name = current_customer_name if current_customer_name != last_customer_name else ''
                    table_data.append([
                        display_customer_id,
                        display_customer_name,
                        row.get('วันที่และเวลา', ''),
                        row.get('ขนาดBandwidth (หน่วย Mbps)', ''),
                        row.get('ปริมาณการใช้งาน incoming (หน่วย bps)', ''),
                        row.get('ปริมาณการใช้งาน outcoming (หน่วย bps)', '')
                    ])
                    last_customer_id = current_customer_id
                    last_customer_name = current_customer_name
                
                # Check if this is the last day's data and monthly averages exist
                is_last_day_group = (date_key == last_date_key)
                if is_last_day_group and monthly_averages:
                    avg_in = monthly_averages['avg_in_month']
                    avg_out = monthly_averages['avg_out_month']
                    
                    # Create a Paragraph for the average text to allow styling
                    average_text_style = ParagraphStyle('AverageText', parent=styles['Normal'])
                    if THAI_FONT_REGISTERED:
                        average_text_style.fontName = THAI_FONT_NAME
                    average_text_style.alignment = 0 # Left align in the cell
                    average_text_style.fontSize = 10 # Match table body font size

                    table_data.append([
                        '', # Empty cell for 'รหัสหน่วยงาน'
                        '', # Empty cell for 'ชื่อหน่วยงาน'
                        Paragraph("<b>Total</b>", average_text_style), # Bold average text
                        '', # Empty cell for 'ขนาดBandwidth'
                        Paragraph(f"<b>{avg_in:,}</b>", average_text_style), # Bold and comma-formatted In_Averagebps
                        Paragraph(f"<b>{avg_out:,}</b>", average_text_style)  # Bold and comma-formatted Out_Averagebps
                    ])
                
                # Define column widths as percentages of available_width
                # This ensures the table expands to fill the page width
                # Total percentages should add up to 1.0 or 100%
                col_widths = [
                    0.10 * available_width,  # รหัสหน่วยงาน (Customer ID)
                    0.29 * available_width,  # ชื่อหน่วยงาน (Customer Name)
                    0.13 * available_width,  # วันที่และเวลา (Date and Time)
                    0.13 * available_width,  # ขนาดBandwidth (Bandwidth)
                    0.18 * available_width,  # ปริมาณการใช้งาน incoming (In usage)
                    0.18 * available_width   # ปริมาณการใช้งาน outcoming (Out usage)
                ]
                
                table = Table(table_data, colWidths=col_widths)
                
                table_style = [
                    ('BACKGROUND', (0, 0), (-1, 0), '#cccccc'),
                    ('TEXTCOLOR', (0, 0), (-1, 0), '#000000'),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('BACKGROUND', (0, 1), (-1, -1), '#f0f0f0'), 
                    ('GRID', (0, 0), (-1, -1), 1, '#999999'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'), # Vertically align text in cells
                ]
                if THAI_FONT_REGISTERED:
                    table_style.append(('FONTNAME', (0, 0), (-1, 0), THAI_FONT_NAME)) # Headers in Thai font
                    table_style.append(('FONTNAME', (0, 1), (-1, -1), THAI_FONT_NAME)) # Body in Thai font
                else:
                    table_style.append(('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold')) # Headers in bold
                    table_style.append(('FONTNAME', (0, 1), (-1, -1), 'Helvetica')) # Body in regular

                # Apply specific style for the last row if it's the monthly average
                if is_last_day_group and monthly_averages:
                    last_row_index = len(table_data) - 1
                    table_style.extend([
                        ('BACKGROUND', (0, last_row_index), (-1, last_row_index), '#d3d3d3'), # Light grey background for average row
                        ('SPAN', (0, last_row_index), (1, last_row_index)), # Span first two columns
                        ('ALIGN', (2, last_row_index), (2, last_row_index), 'LEFT'), # Align 'Total' text left
                        ('FONTNAME', (2, last_row_index), (2, last_row_index), THAI_FONT_NAME if THAI_FONT_REGISTERED else 'Helvetica-Bold'),
                        ('FONTNAME', (4, last_row_index), (4, last_row_index), THAI_FONT_NAME if THAI_FONT_REGISTERED else 'Helvetica-Bold'),
                        ('FONTNAME', (5, last_row_index), (5, last_row_index), THAI_FONT_NAME if THAI_FONT_REGISTERED else 'Helvetica-Bold'),
                        ('ALIGN', (4, last_row_index), (4, last_row_index), 'CENTER'), # Center average values
                        ('ALIGN', (5, last_row_index), (5, last_row_index), 'CENTER'), # Center average values
                        ('BOTTOMPADDING', (0, last_row_index), (-1, last_row_index), 12),
                        ('TOPPADDING', (0, last_row_index), (-1, last_row_index), 12),
                    ])

                table.setStyle(table_style)
                elements.append(table)
                elements.append(Spacer(1, 0.5 * inch))

                first_page = False
        else:
            no_data_style = styles['Normal']
            if THAI_FONT_REGISTERED:
                no_data_style.fontName = THAI_FONT_NAME
            elements.append(Paragraph("No circuit status data available.", no_data_style))
        
        # Build the document with the defined margins
        doc.leftMargin = left_margin
        doc.rightMargin = right_margin
        doc.topMargin = 0.5 * inch # Set top margin
        doc.bottomMargin = 0.5 * inch # Set bottom margin

        doc.build(elements)
        logger.info(f"✅ สร้าง PDF สำหรับ '{node_name}' สำเร็จแล้ว")
        return True, "PDF generated successfully."
    except Exception as e:
        logger.error(f"❌ สร้าง PDF สำหรับ '{node_name}' ล้มเหลว: {e}")
        return False, f"Error generating PDF: {e}"

def process_file_in_background(file_stream, job_id):
    """
    ฟังก์ชันนี้จะทำงานในอีก Thread หนึ่ง
    โดยจะรับ file_stream (ข้อมูลไฟล์) และ job_id มาประมวลผล
    """
    temp_dir = None # โฟลเดอร์สำหรับ CSV/PDF ย่อย
    try:
        df = pd.read_excel(file_stream)
        total_rows = len(df)
        with status_lock:
            processing_status[job_id]['total'] = total_rows
            processing_status[job_id]['results'] = []
            # สร้าง directory ชั่วคราวสำหรับเก็บไฟล์ CSV/PDF ของงานนี้
            temp_dir = tempfile.mkdtemp(prefix=f"report_job_{job_id}_")
            processing_status[job_id]['temp_dir'] = temp_dir # เก็บ temp_dir ไว้ในสถานะ
        
        logger.info(f"📊 เริ่มประมวลผลไฟล์ Excel มีทั้งหมด {total_rows} รายการ")

        required_columns = ['NodeID', 'Interface ID', 'กระทรวง / สังกัด', 'กรม / สังกัด', 'จังหวัด', 'ชื่อหน่วยงาน', 'Node Name']
        if not all(col in df.columns for col in required_columns):
            missing_cols = [c for c in required_columns if c not in df.columns]
            with status_lock:
                processing_status[job_id]['error'] = f"ไฟล์ Excel ขาดคอลัมน์ที่จำเป็น: {', '.join(missing_cols)}"
                processing_status[job_id]['completed'] = True
            logger.error(f"❌ {processing_status[job_id]['error']}")
            return
        
        # ใช้ temp_dir เป็น root สำหรับการบันทึกไฟล์ชั่วคราว
        csv_root_dir = os.path.join(temp_dir, 'csv')
        pdf_root_dir = os.path.join(temp_dir, 'pdf')
        os.makedirs(csv_root_dir, exist_ok=True)
        os.makedirs(pdf_root_dir, exist_ok=True)
        
        for index, row in df.iterrows():
            with status_lock:
                if processing_status[job_id].get('canceled'):
                    logger.info(f"⛔ งานถูกยกเลิกโดยผู้ใช้")
                    break
            
            node_name = ''
            csv_success = False
            pdf_success = False
            error_message = None

            try:
                nod_id = str(row['NodeID']).strip()
                itf_id = str(row['Interface ID']).strip()
                
                folder1 = str(row['กระทรวง / สังกัด']).strip()
                folder2 = str(row['กรม / สังกัด']).strip()
                folder3 = str(row['จังหวัด']).strip()
                folder4 = str(row['ชื่อหน่วยงาน']).strip()
                node_name = str(row['Node Name']).strip()

                if not nod_id or not itf_id:
                    error_message = "ข้อมูล NodeID หรือ Interface ID ไม่สมบูรณ์"
                    logger.warning(f"⚠️ ข้ามแถวที่ {index + 1} เนื่องจาก {error_message} (NodeID: '{nod_id}', ITF ID: '{itf_id}')")
                    with status_lock:
                        processing_status[job_id]['processed'] += 1
                        processing_status[job_id]['results'].append({
                            'node_name': node_name,
                            'csv_success': False,
                            'pdf_success': False,
                            'error_message': error_message
                        })
                    continue
                
                logger.info(f"▶ กำลังประมวลผล NodeID: {nod_id}, Interface ID: {itf_id} (แถวที่ {index + 1})")

                # สร้าง sub-directory ภายใต้ temp_dir
                current_csv_dir = os.path.join(csv_root_dir, folder1, folder2, folder3, folder4)
                current_pdf_dir = os.path.join(pdf_root_dir, folder1, folder2, folder3, folder4)
                
                os.makedirs(current_csv_dir, exist_ok=True)
                os.makedirs(current_pdf_dir, exist_ok=True)
                
                raw_json_data = get_data_from_api(nod_id, itf_id, job_id)

                if raw_json_data:
                    headers, processed_data, monthly_averages = process_json_data(raw_json_data, job_id)
                    
                    # Clean node_name for filenames
                    sanitized_node_name = re.sub(r'[\\/:*?"<>|]', '_', node_name)
                    # MODIFIED: เพิ่ม ID เพื่อให้ไฟล์ชื่อไม่ซ้ำกันในกรณีที่ Node Name ซ้ำ
                    # OLD: filename_base = f"{sanitized_node_name}_{nod_id}_{itf_id}" 
                    # NEW: ให้เป็นแค่ Node Name
                    filename_base = f"{sanitized_node_name}" 

                    csv_filename = os.path.join(current_csv_dir, f"{filename_base}.csv")
                    pdf_filename = os.path.join(current_pdf_dir, f"{filename_base}.pdf")

                    csv_success, csv_msg = export_to_csv(headers, processed_data, monthly_averages, csv_filename, job_id, node_name)
                    pdf_success, pdf_msg = export_to_pdf(headers, processed_data, monthly_averages, pdf_filename, job_id, node_name)
                else:
                    error_message = f"ไม่สามารถดึงข้อมูลจาก API ได้สำหรับ NodeID: {nod_id}, Interface ID: {itf_id}"
                    logger.error(f"❌ {error_message}")
            
            except Exception as e:
                error_message = f"เกิดข้อผิดพลาดที่ไม่คาดคิดในแถวที่ {index + 1}: {e}"
                logger.error(f"❌ {error_message}")
                
            finally:
                with status_lock:
                    processing_status[job_id]['processed'] += 1
                    processing_status[job_id]['results'].append({
                        'node_name': node_name,
                        'csv_success': csv_success,
                        'pdf_success': pdf_success,
                        'error_message': error_message
                    })
        
        # หลังประมวลผลทั้งหมด สร้างไฟล์ ZIP
        if not processing_status[job_id].get('canceled'):
            zip_filename = f"customer_reports_{job_id}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
            # ให้ zip_file_path ชี้ไปที่ tempfile.gettempdir() โดยตรง
            zip_file_path = os.path.join(tempfile.gettempdir(), zip_filename)
            
            if temp_dir and os.path.exists(temp_dir):
                with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(temp_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            # สร้าง relative path ภายใน zip (ตัด temp_dir ออก)
                            arcname = os.path.relpath(file_path, temp_dir)
                            zipf.write(file_path, arcname)
                
                with status_lock:
                    processing_status[job_id]['zip_file_path'] = zip_file_path
                    processing_status[job_id]['completed'] = True
                logger.info(f"✅ การสร้างรายงานเสร็จสมบูรณ์! ไฟล์ ZIP: {zip_file_path.split(os.sep)[-1]}")
            else:
                with status_lock:
                    processing_status[job_id]['error'] = "ไม่พบโฟลเดอร์ชั่วคราวสำหรับสร้าง ZIP"
                    processing_status[job_id]['completed'] = True
                logger.error(f"❌ ไม่พบโฟลเดอร์ชั่วคราว '{temp_dir}' ไม่สามารถสร้าง ZIP ได้")
        else:
            with status_lock:
                 processing_status[job_id]['completed'] = True
                 processing_status[job_id]['error'] = "การประมวลผลถูกยกเลิก"

    except Exception as e:
        with status_lock:
            processing_status[job_id]['error'] = f"เกิดข้อผิดพลาดในระหว่างการประมวลผลเบื้องหลัง: {e}"
            processing_status[job_id]['completed'] = True
        logger.critical(f"❌ {processing_status[job_id]['error']}")
    finally:
        # **สำคัญ:** ลบเฉพาะโฟลเดอร์ชั่วคราวสำหรับ CSV/PDF (temp_dir)
        # ไฟล์ ZIP จะถูกเก็บไว้ใน tempfile.gettempdir() และไม่ถูกลบจากที่นี่
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"📁 ลบโฟลเดอร์ CSV/PDF ชั่วคราว: {temp_dir.split(os.sep)[-1]} แล้ว (ไม่รวมไฟล์ ZIP)")
            except Exception as e:
                logger.error(f"❌ ข้อผิดพลาดในการลบโฟลเดอร์ CSV/PDF ชั่วค้าง: {e}")
        
        # *** สำคัญมาก: ไม่มีการลบ job_id ออกจาก processing_status ที่นี่แล้ว ***
        # เพื่อให้สามารถดาวน์โหลดไฟล์ ZIP ซ้ำได้
        # แต่คุณจะต้องจัดการการล้างสถานะและไฟล์ ZIP เก่าๆ ด้วยตัวเองในภายหลัง
        # ดูฟังก์ชัน `cleanup_old_jobs` ด้านล่างเป็นตัวอย่าง


# --- Route สำหรับ Flask App ---
@app.route('/')
def upload_form():
    """แสดงหน้าฟอร์มสำหรับอัปโหลดไฟล์ Excel"""
    return render_template('index.html')

@app.route('/generate_report', methods=['POST'])
def generate_report():
    """
    รับไฟล์ที่อัปโหลด แล้วเริ่มการประมวลผลในเบื้องหลัง
    """
    if 'excel_file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['excel_file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if file:
        job_id = str(uuid.uuid4())
        file_stream = io.BytesIO(file.read())
        
        with status_lock:
            processing_status[job_id] = {
                'total': -1, 
                'processed': 0, 
                'completed': False, 
                'error': None,
                'canceled': False,
                'results': [],
                'temp_dir': None,       # เก็บ directory ชั่วคราว (สำหรับ CSV/PDF)
                'zip_file_path': None,  # เก็บ path ของไฟล์ zip
                'timestamp': datetime.datetime.now() # เพิ่ม timestamp สำหรับการล้างข้อมูลในอนาคต
            }
        logger.info(f"📂 ได้รับไฟล์ excel '{file.filename}' และเริ่มการประมวลผล (Job ID: {job_id})")

        thread = threading.Thread(target=process_file_in_background, args=(file_stream, job_id))
        thread.daemon = True # ทำให้ thread จบเมื่อ process หลักจบ
        thread.start()
        
        return jsonify({"message": "Processing started", "job_id": job_id})

@app.route('/status/<job_id>')
def get_status(job_id):
    """
    ตรวจสอบสถานะของงานที่กำลังประมวลผลอยู่
    """
    with status_lock:
        status = processing_status.get(job_id, {})
    return jsonify(status)

@app.route('/logs/<job_id>')
def get_logs(job_id):
    """
    ดึง log ของงานที่กำลังประมวลผลอยู่
    """
    logs = []
    # ดึง log ออกจาก queue จนกว่าจะว่างเปล่า
    while not log_queue.empty():
        try:
            logs.append(log_queue.get_nowait())
        except Exception:
            break
    return jsonify({"logs": logs})


@app.route('/cancel/<job_id>', methods=['POST'])
def cancel_job(job_id):
    """
    รับคำสั่งยกเลิกงานที่กำลังประมวลผลอยู่
    """
    with status_lock:
        if job_id in processing_status:
            processing_status[job_id]['canceled'] = True
            logger.info(f"⛔ ได้รับคำขอยกเลิกงาน (Job ID: {job_id})")
            return jsonify({"message": "Job cancellation requested"}), 200
        else:
            logger.warning(f"⚠️ พยายามยกเลิกงานที่ไม่พบ (Job ID: {job_id})")
            return jsonify({"error": "Job not found"}), 404

@app.route('/download_report/<job_id>')
def download_report(job_id):
    """
    ให้ผู้ใช้ดาวน์โหลดไฟล์ ZIP ที่สร้างขึ้น
    """
    with status_lock:
        job_info = processing_status.get(job_id)

    if not job_info:
        logger.error(f"❌ ไม่พบข้อมูลงานสำหรับดาวน์โหลด (Job ID: {job_id})")
        return jsonify({"error": "Job not found or not ready for download. It might be too old or cancelled."}), 404

    zip_file_path = job_info.get('zip_file_path')

    if not zip_file_path or not os.path.exists(zip_file_path):
        logger.error(f"❌ ไม่พบไฟล์ ZIP หรือยังสร้างไม่เสร็จ (Job ID: {job_id}). Path: {zip_file_path}")
        if job_info.get('completed') and not zip_file_path:
            return jsonify({"error": "Report completed with no ZIP file generated (internal error)"}), 500
        return jsonify({"error": "Report not yet generated or file not found"}), 404
    
    try:
        directory = tempfile.gettempdir()
        filename = os.path.basename(zip_file_path)
        logger.info(f"📥 กำลังส่งไฟล์ ZIP: {filename} จาก {directory} (Job ID: {job_id})")
        
        # MODIFIED: กำหนดชื่อไฟล์ ZIP ที่ผู้ใช้จะดาวน์โหลด
        current_date_str = datetime.datetime.now().strftime('%Y%m%d') # รูปแบบ ปีเดือนวัน
        download_filename = f"Solarwind_{current_date_str}.zip"

        response = send_from_directory(
            directory=directory,
            path=filename,
            as_attachment=True,
            mimetype='application/zip',
            download_name=download_filename # ใช้ชื่อไฟล์ใหม่ที่กำหนด
        )
        
        return response

    except Exception as e:
        logger.critical(f"❌ ข้อผิดพลาดร้ายแรงในการส่งไฟล์ ZIP: {e} (Job ID: {job_id})")
        return jsonify({"error": f"Failed to serve file: {e}"}), 500

# --- ฟังก์ชันสำหรับล้างข้อมูลเก่า (ควรนำไปใช้ใน Production) ---
def cleanup_old_jobs():
    """
    ลบสถานะงานและไฟล์ ZIP เก่าๆ ออกจากระบบ
    รันเป็น background process
    """
    logger.info("🧹 เริ่มต้นกระบวนการล้างข้อมูลงานเก่า...")
    current_time = datetime.datetime.now()
    jobs_to_remove = []

    retention_hours = 24  # กำหนดเวลาที่ต้องการเก็บไฟล์ (เช่น 24 ชั่วโมง) # ควรปรับค่านี้ตามความเหมาะสมของการใช้งานและพื้นที่ดิสก์
    retention_seconds = retention_hours * 3600

    with status_lock:
        # ใช้ list() เพื่อสร้างสำเนาของ keys/items เพื่อป้องกัน RuntimeError: dictionary changed size during iteration
        for job_id, job_info in list(processing_status.items()): 
            # ลบงานที่เสร็จสมบูรณ์แล้วและเกินเวลาที่กำหนด
            # หรือลบงานที่เกิดข้อผิดพลาดและนานเกินไป
            if job_info.get('completed') and job_info.get('timestamp'): 
                job_timestamp = job_info['timestamp']
                if (current_time - job_timestamp).total_seconds() > retention_seconds:
                    jobs_to_remove.append(job_id)
            elif (not job_info.get('completed')) and (current_time - job_info.get('timestamp', current_time)).total_seconds() > (retention_seconds / 4): # หากยังไม่เสร็จ อาจจะลบเร็วขึ้น
                # อาจจะเพิ่ม logic สำหรับงานที่ค้างนานเกินไปและยังไม่เสร็จสมบูรณ์
                logger.warning(f"⚠️ พบงานค้างเก่า (ไม่สมบูรณ์) กำลังถูกลบ: {job_id}")
                jobs_to_remove.append(job_id)


    for job_id in jobs_to_remove:
        with status_lock:
            # ใช้ .pop() เพื่อลบ key ออกจาก dictionary พร้อมกับได้ value กลับมา
            job_info = processing_status.pop(job_id, None) 
        if job_info:
            zip_file_path = job_info.get('zip_file_path')
            if zip_file_path and os.path.exists(zip_file_path):
                try:
                    os.remove(zip_file_path)
                    logger.info(f"🗑️ ลบไฟล์ ZIP เก่า: {os.path.basename(zip_file_path)} (Job ID: {job_id})")
                except Exception as e:
                    logger.error(f"❌ ข้อผิดพลาดในการลบไฟล์ ZIP เก่า: {e} (Job ID: {job_id})")
            logger.info(f"✨ ล้างสถานะงานสำหรับ Job ID: {job_id} แล้ว")
    logger.info("🧹 กระบวนการล้างข้อมูลงานเก่าเสร็จสมบูรณ์")
    # ตั้งเวลาเรียกตัวเองใหม่
    threading.Timer(retention_seconds / 2, cleanup_old_jobs).start() # รันบ่อยขึ้นเล็กน้อย (เช่น ทุกๆ 12 ชั่วโมง)

# --- ส่วนของการรัน Flask App ---
if __name__ == '__main__':
    cleanup_thread = threading.Thread(target=cleanup_old_jobs)
    cleanup_thread.daemon = True # ทำให้ thread จบเมื่อ process หลักจบ
    cleanup_thread.start()

    app.run(debug=True) # debug=True จะช่วยในการพัฒนา แต่ไม่ควรใช้ใน Production
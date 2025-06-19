import os
import time
import datetime
import zipfile
import smtplib
import ssl
from email.message import EmailMessage
from pymodbus.client import ModbusTcpClient
from openpyxl import load_workbook

# === Ayarlar ===
log_dir = "/home/testonaylab/projeler/sensor"
email_sender = "testonayraspberrypi@gmail.com"
email_password = "myyo tnqh idwl qgkb"

gazlab_mail = "fatih.cilesiz@beko.com"
elektriklab_mail = "ferhat_bicer@beko.com"

sensors = {
    "GazLab": {
        "ip": "10.114.8.251",
        "registers": [48, 49, 50],
        "email": gazlab_mail
    },
    "ElektrikLab": {
        "ip": "10.114.8.223",
        "registers": [48, 49, 50],
        "email": elektriklab_mail
    },
    "FırınPerformansLab": {
        "ip": "10.114.8.136",
        "registers": [48, 49, 50],
        "email": elektriklab_mail
    }
}

def read_values(ip, registers):
    values = []
    client = ModbusTcpClient(host=ip, port=502)
    try:
        if not client.connect():
            raise ConnectionError(f"{ip} bağlantı başarısız")

        for reg in registers:
            result = client.read_holding_registers(address=reg, count=1, slave=1)
            if not result.isError():
                raw = result.registers[0]
                values.append(round(raw / 10.0, 1))
            else:
                values.append(None)
    except Exception as e:
        print(f"[HATA] {ip} için veri okunamadı → {e}")
        values = [None] * len(registers)
    finally:
        client.close()
    return values

def log_data(lab_name, values):
    today = time.strftime("%d-%m-%Y")
    filename = f"sensor_log_{today}_{lab_name}.txt"
    filepath = os.path.join(log_dir, filename)

    if not os.path.exists(filepath):
        with open(filepath, 'w') as f:
            f.write("Zaman\tSıcaklık (\u00b0C)\tNem (%)\tYo\u011fu\u015fma Noktasi (\u00b0C)\n")

    now = time.strftime("%H:%M:%S")
    formatted_values = [str(v) if v is not None else "None" for v in values]
    line = f"{now}\t{formatted_values[0]}\t{formatted_values[1]}\t{formatted_values[2]}\n"

    with open(filepath, 'a') as f:
        f.write(line)

    print(f"{lab_name} → {line.strip()}")

def populate_excel_template(txt_path, lab_name, output_path):
    today = datetime.date.today().strftime("%d-%m-%Y")

    with open(txt_path, 'r') as f:
        lines = f.readlines()

    if len(lines) < 2:
        print("Boş ya da başlıksız log dosyası.")
        return False

    data = [line.strip().split('\t') for line in lines[1:]]
    template_path = os.path.join(log_dir, "Template_Sheet.xlsx")
    wb = load_workbook(template_path)
    ws = wb.active

    ws["C1"] = lab_name
    ws["D1"] = today

    for i, row in enumerate(data):
        ws.cell(row=3 + i, column=1, value=row[0])
        ws.cell(row=3 + i, column=2, value=float(row[1]) if row[1] != "None" else None)
        ws.cell(row=3 + i, column=3, value=float(row[2]) if row[2] != "None" else None)
        ws.cell(row=3 + i, column=4, value=float(row[3]) if row[3] != "None" else None)

    wb.save(output_path)
    return True

def send_weekly_zip_and_clean():
    now = datetime.datetime.now()
    if now.weekday() != 6 or now.hour != 12:  # Pazar saat 12 değilse çık
        return

    year, week, _ = now.isocalendar()

    for lab_name in sensors:
        zip_name = f"week_{year}_{week}_{lab_name}.zip"
        zip_path = os.path.join(log_dir, zip_name)

        if os.path.exists(zip_path):
            print(f"{lab_name} için zip dosyası zaten var, tekrar gönderilmeyecek.")
            continue

        txt_files = [
            os.path.join(log_dir, f)
            for f in os.listdir(log_dir)
            if f.startswith("sensor_log_") and f.endswith(f"_{lab_name}.txt")
        ]

        xlsx_files = []

        for txt_file in txt_files:
            date_part = os.path.basename(txt_file).split("_")[2]
            xlsx_name = f"sensor_log_{date_part}_{lab_name}.xlsx"
            xlsx_path = os.path.join(log_dir, xlsx_name)
            success = populate_excel_template(txt_file, lab_name, xlsx_path)
            if success:
                xlsx_files.append(xlsx_path)

        if not xlsx_files:
            continue

        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for file in xlsx_files:
                zipf.write(file, arcname=os.path.basename(file))

        msg = EmailMessage()
        msg['Subject'] = f"Haftalık Log - {lab_name} - Hafta {week}, {year}"
        msg['From'] = email_sender
        msg['To'] = sensors[lab_name]['email']
        msg.set_content(f"{lab_name} logları ekte.")

        with open(zip_path, 'rb') as f:
            msg.add_attachment(f.read(), maintype='application', subtype='zip', filename=zip_name)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email_sender, email_password)
            smtp.send_message(msg)
            print(f"{lab_name} → Mail gönderildi")

        for file in txt_files + xlsx_files:
            os.remove(file)
            print(f"{lab_name} → Silindi: {file}")

# === Ana Döngü ===
try:
    while True:
        for lab_name, config in sensors.items():
            values = read_values(config['ip'], config['registers'])
            log_data(lab_name, values)

        send_weekly_zip_and_clean()

        now = datetime.datetime.now()
        next_minute = (now + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
        time.sleep((next_minute - now).total_seconds())

except KeyboardInterrupt:
    print("Loglama durduruldu.")

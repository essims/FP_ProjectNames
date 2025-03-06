# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 16:07:57 2025

@author: esra.simsek
"""

import psycopg2
import pandas as pd
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from pathlib import Path

# ✅ Get the directory where the script is located
BASE_DIR = Path(__file__).resolve().parent

# ✅ Load the .env file relative to the script's location
env_path = BASE_DIR / ".env"

print(f"🔍 Debug: Looking for .env at: {env_path}")

# ✅ Load .env with error handling
if env_path.exists():
    load_dotenv(env_path, override=True)
    print("✅ .env file loaded successfully.")
else:
    print(f"❌ Failed to load .env file. Make sure it exists at: {env_path}")


# ✅ Database Configuration
DB_CONFIG = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST"),
    "port": os.environ.get("DB_PORT")
}

# ✅ Email Settings
SMTP_SERVER = os.environ.get("SMTP_SERVER")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
EMAIL_SENDER = os.environ.get("EMAIL_SENDER", "").strip()
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "").strip()

# ✅ Ensure multiple recipients are read correctly
EMAIL_RECEIVER_RAW = os.environ.get("EMAIL_RECEIVER", "")
EMAIL_RECEIVER = [email.strip() for email in EMAIL_RECEIVER_RAW.split(",") if email.strip()]

print(f"✅ Email will be sent to: {EMAIL_RECEIVER}")

# ✅ Database Connection Test
print("Step 3: Testing database connection...")
try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.close()
    print("✅ Database connection successful.")
except Exception as e:
    print(f"❌ Database connection failed: {e}")

# ✅ CSV Directory (Change as needed)
CSV_DIR = Path(r"L:\Sample Inventory Department\FreezerPro Projects")


# ✅ Define file names for comparison
today = datetime.now().strftime("%Y-%m-%d")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
yesterday_file = CSV_DIR / f"project_names_{yesterday}.csv"
today_file = CSV_DIR / f"project_names_{today}.csv"

# ✅ Fetch project names from the database
def fetch_project_names():
    print("Step 4: Fetching project names from database...")
    query = """
    SELECT DISTINCT value AS project_names
    FROM str_values
    WHERE property_id = 78
    ORDER BY value ASC
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        df = pd.DataFrame(data, columns=["project_names"])
        cur.close()
        conn.close()
        print("✅ Project names fetched successfully.")
        return df
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return None

# ✅ Load previous CSV for comparison
def load_csv(filepath):
    print(f"Step 5: Checking if previous CSV file exists: {filepath}")
    if os.path.exists(filepath):
        df = pd.read_csv(filepath, usecols=["project_names"], dtype=str)
        df = df.dropna(subset=["project_names"])
        print("✅ Previous CSV file loaded successfully.")
        return df
    print("⚠️ No previous CSV file found.")
    return pd.DataFrame(columns=["project_names"])

# ✅ Compare today's and yesterday's data
def compare_csvs(new_df, old_df):
    print("Step 6: Comparing today's project names with yesterday's...")

    # Normalize spaces and case sensitivity
    old_projects_normalized = set(old_df["project_names"].str.strip().str.lower())
    new_projects_normalized = set(new_df["project_names"].str.strip().str.lower())

    added_projects = new_projects_normalized - old_projects_normalized
    removed_projects = old_projects_normalized - new_projects_normalized

    # Convert back to original names for reporting
    added_df = new_df[new_df["project_names"].str.strip().str.lower().isin(added_projects)]
    removed_df = old_df[old_df["project_names"].str.strip().str.lower().isin(removed_projects)]

    diff_df = pd.concat([added_df.assign(status="Added"), removed_df.assign(status="Removed")])

    if not diff_df.empty:
        print("✅ Changes detected in project names.")
    else:
        print("⚠️ No changes in project names.")

    return diff_df

# ✅ Send email notification
def send_email(total_projects, diff_df):
    print("Step 7: Preparing email notification...")
    subject = f"Daily FP Project Name Report - {today}"
    body = f"Total FP projects today: {total_projects}\n\n"
    
    if diff_df.empty:
        body += "No changes in FP project names."
    else:
        body += "Changes in FP project names:\n"
        body += diff_df.to_string(index=False)

    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_RECEIVER)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    print(f"🔍 Debug: Email will be sent to: {EMAIL_RECEIVER}")

    try:
        print("Step 8: Sending email...")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)

        # ✅ Ensure all recipients correctly receive the email
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())

        server.quit()
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")

# ✅ Run the workflow
new_data = fetch_project_names()
old_data = load_csv(yesterday_file)

if new_data is not None:
    diff_df = compare_csvs(new_data, old_data)
    
    print("Step 9: Saving CSV file (archiving)...")
    CSV_DIR.mkdir(parents=True, exist_ok=True)  # Ensure CSV directory exists
    new_data.sort_values(by="project_names", inplace=True)
    new_data.to_csv(today_file, index=False)
    print(f"✅ CSV file saved: {today_file}")

    total_projects = len(new_data)
    send_email(total_projects, diff_df)

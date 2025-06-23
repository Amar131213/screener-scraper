# ------------------------ REQUIRED LIBRARIES ------------------------
from fastapi import FastAPI
import threading
import pandas as pd
import gspread
import requests
from bs4 import BeautifulSoup
import time
from io import StringIO
from google.oauth2.service_account import Credentials
import uvicorn

app = FastAPI()

# ------------------------ GOOGLE SHEET AUTHENTICATION -------------------------
SERVICE_ACCOUNT_FILE = 'service_account.json'  # ← ये फाइल Render पर रखें
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

# ------------------------ LOGIN FUNCTION ------------------------
def login_to_screener(session, username, password):
    try:
        login_url = 'https://www.screener.in/login/'
        res = session.get(login_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']

        payload = {
            'csrfmiddlewaretoken': csrf_token,
            'username': username,
            'password': password,
            'next': ''
        }

        headers = {
            'Referer': 'https://www.screener.in/login/',
            'User-Agent': 'Mozilla/5.0'
        }

        res2 = session.post(login_url, data=payload, headers=headers)
        return 'Core Watchlist' in res2.text
    except Exception as e:
        print(f"Login error: {e}")
        return False

# ------------------------ RETRY FUNCTION FOR FETCH ------------------------
def fetch_data_with_retry(session, url, retries=10, delay=1):
    for attempt in range(retries):
        try:
            response = session.get(url)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            if attempt < retries - 1:
                print(f"[Retry {attempt + 1}] {e}. Waiting {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"❌ Failed after {retries} retries: {e}")
                return None

# ------------------------ ACCOUNT LIST ------------------------
accounts = [
    {"username": "amarbhavsarb@gmail.com",     "password": "abcd@0000", "url": "https://www.screener.in/screens/1790669/ttyy/?page={}", "range": "A1:T6000",  "add_classification": True},
    {"username": "amarbhavsarb+2@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/1790603/ttyy/?page={}", "range": "Z1:AQ6000", "add_classification": False},
    {"username": "amarbhavsarb+3@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/1790798/ttyy/?page={}", "range": "AY1:BP6000", "add_classification": False},
    {"username": "amarbhavsarb+4@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/2113854/ttyy/?page={}", "range": "BX1:CO6000", "add_classification": False},
    {"username": "amarbhavsarb+5@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/2358928/ttyy/?page={}", "range": "CW1:DN6000", "add_classification": False},
]

# ------------------------ SPREADSHEET SETUP ------------------------
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1IHZkyzSnOcNphq9WO9pkyTaTkAO_P1eJ3mHb2VnCkJI/edit#gid=0'

# ------------------------ MAIN SCRAPER FUNCTION ------------------------
def run_scraper():
    sheet = gc.open_by_url(spreadsheet_url).worksheet('Sheet2')
    for idx, acc in enumerate(accounts):
        print(f"\n🚀 Scraping Account {idx+1}: {acc['username']}")
        session = requests.Session()

        if login_to_screener(session, acc['username'], acc['password']):
            print("✅ Login successful")
            all_data = []
            page = 1

            while True:
                page_url = acc['url'].format(page)
                response = fetch_data_with_retry(session, page_url, retries=10, delay=1)
                if not response:
                    print(f"⚠️ Skipping page {page} due to repeated failures.")
                    break

                try:
                    tables = pd.read_html(StringIO(response.text), header=0)
                except:
                    print(f"⚠️ Failed to parse table on page {page}")
                    break

                if tables:
                    df = tables[0].fillna('')

                    if acc["add_classification"]:
                        df['Classification'] = None
                        df['Hyperlink'] = None

                        soup = BeautifulSoup(response.content, 'html.parser')
                        rows = soup.find('table', class_='data-table').find('tbody').find_all('tr')

                        for i, row in enumerate(rows):
                            cols = row.find_all('td')
                            if len(cols) > 1:
                                try:
                                    value = float(cols[5].text.replace(',', ''))
                                    classification = None
                                    if 0.01 <= value <= 99.99:
                                        classification = 1
                                    elif 100 <= value <= 999.99:
                                        classification = 2
                                    elif 1000 <= value <= 99999.99:
                                        classification = 3
                                    elif value >= 100000:
                                        classification = 4
                                    if i > 0:
                                        df.iloc[i - 1, -2] = classification
                                except:
                                    df.iloc[i - 1, -2] = None

                                name_column = cols[1]
                                name_link = name_column.find('a')['href'] if name_column.find('a') else ''
                                embedded_link = f'https://www.screener.in{name_link}'
                                hyperlink_formula = f'=HYPERLINK("{embedded_link}", "{name_column.text.strip()}")'
                                if i > 0:
                                    df.iloc[i - 1, -1] = hyperlink_formula

                        df = df[[*df.columns[:-2], 'Classification', 'Hyperlink']]

                        if 'Down  %' in df.columns:
                            df['Down  %'] = df['Down  %'].apply(
                                lambda x: f'-{float(x)}' if str(x).replace('.', '', 1).isdigit() else x
                            )
                    else:
                        df = df.iloc[:, :18]

                    blank_row = [""] * len(df.columns)
                    all_data += [df.columns.tolist()] + df.values.tolist() + [blank_row]
                    print(f"✅ Page {page} scraped.")
                else:
                    print(f"⚠️ No tables on page {page}. Ending scraping.")
                    break

                if 'Next' not in response.text:
                    break
                page += 1
                time.sleep(0.7)

            try:
                sheet.batch_clear([acc['range']])
                sheet.update(values=all_data, range_name=acc['range'], value_input_option='USER_ENTERED')
                print(f"✅ Data written to Google Sheet range: {acc['range']}")
            except Exception as e:
                print(f"❌ Sheet update failed: {e}")
        else:
            print("❌ Login failed")

    # Optional: Trigger Google Apps Script
    print("\n🔔 Triggering Google Apps Script...")
    time.sleep(10)
    apps_script_url = 'https://script.google.com/macros/s/AKfycbwXBBIXOmlltYWprwFzVg0tWvtPlT-eSKZBBTzSF_pzxKUbqpLJUHWQ7P_R_EIdcVHusw/exec'
    try:
        final_response = requests.get(apps_script_url)
        if final_response.status_code == 200:
            print("✅ Google Apps Script function triggered successfully.")
        else:
            print(f"❌ Script trigger failed: {final_response.status_code}")
    except Exception as e:
        print(f"❌ Error calling Google Apps Script: {e}")

# ------------------------ FASTAPI ENDPOINTS ------------------------
@app.get("/")
def home():
    return {"message": "✅ Scraper is alive!"}

@app.get("/run")
def run():
    threading.Thread(target=run_scraper).start()
    return {"status": "🟢 Scraper started in background."}

# ------------------------ MAIN ------------------------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)

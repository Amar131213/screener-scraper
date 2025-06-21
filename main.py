from flask import Flask
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import time
from io import StringIO

app = Flask(__name__)

# ------------------------ GOOGLE SHEET SETUP ------------------------

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Make sure it's in root

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1IHZkyzSnOcNphq9WO9pkyTaTkAO_P1eJ3mHb2VnCkJI/edit#gid=0'
sheet = gc.open_by_url(spreadsheet_url).worksheet('Sheet2')

# ------------------------ LOGIN TO SCREENER ------------------------

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

def fetch_data_with_retry(session, url, retries=10, delay=1):
    for attempt in range(retries):
        try:
            response = session.get(url)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"[Retry {attempt + 1}] {e}")
            time.sleep(delay)
    return None

accounts = [
    {"username": "amarbhavsarb@gmail.com",     "password": "abcd@0000", "url": "https://www.screener.in/screens/1790669/ttyy/?page={}", "range": "A1:T6000",  "add_classification": True},
    {"username": "amarbhavsarb+2@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/1790603/ttyy/?page={}", "range": "Z1:AQ6000", "add_classification": False},
    {"username": "amarbhavsarb+3@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/1790798/ttyy/?page={}", "range": "AY1:BP6000", "add_classification": False},
    {"username": "amarbhavsarb+4@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/2113854/ttyy/?page={}", "range": "BX1:CO6000", "add_classification": False},
    {"username": "amarbhavsarb+5@gmail.com",   "password": "abcd@0000", "url": "https://www.screener.in/screens/2358928/ttyy/?page={}", "range": "CW1:DN6000", "add_classification": False},
]

@app.route('/run')
def run_scraper():
    for idx, acc in enumerate(accounts):
        print(f"\n🚀 Scraping Account {idx+1}: {acc['username']}")
        session = requests.Session()

        if not login_to_screener(session, acc['username'], acc['password']):
            print("❌ Login failed")
            continue

        all_data = []
        page = 1

        while True:
            page_url = acc['url'].format(page)
            response = fetch_data_with_retry(session, page_url)
            if not response:
                break

            try:
                tables = pd.read_html(StringIO(response.text), header=0)
            except:
                break

            if not tables:
                break

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
                        hyperlink_formula = f'=HYPERLINK("https://www.screener.in{name_link}", "{name_column.text.strip()}")'
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

            if 'Next' not in response.text:
                break
            page += 1
            time.sleep(0.7)

        try:
            sheet.batch_clear([acc['range']])
            sheet.update(values=all_data, range_name=acc['range'], value_input_option='USER_ENTERED')
            print(f"✅ Sheet updated: {acc['range']}")
        except Exception as e:
            print(f"❌ Sheet update failed: {e}")

    # Optional: Trigger Apps Script
    try:
        requests.get("https://script.google.com/macros/s/AKfycbwXBBIXOmlltYWprwFzVg0tWvtPlT-eSKZBBTzSF_pzxKUbqpLJUHWQ7P_R_EIdcVHusw/exec")
    except:
        print("⚠️ Apps Script call failed")

    return '✅ Scraping done!'

# ------------------------ RENDER ENTRY ------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

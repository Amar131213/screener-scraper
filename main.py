import threading
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import requests
from bs4 import BeautifulSoup
import time
from io import StringIO
from fastapi import FastAPI
import uvicorn

app = FastAPI()
SERVICE_ACCOUNT_FILE = 'service_account.json'  # Render पर local रखो
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)
session = requests.Session()

username = 'amarbhavsarb@gmail.com'
password = 'abcd@0000'
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1IHZkyzSnOcNphq9WO9pkyTaTkAO_P1eJ3mHb2VnCkJI/edit#gid=0'
sheet_name = 'Sheet2'
url_base = 'https://www.screener.in/screens/1790669/ttyy/?page={}'

@app.get("/")
def root():
    return {"status": "Scraper is alive"}

def login_to_screener(username, password):
    try:
        url_login = 'https://www.screener.in/login/'
        login_page_response = session.get(url_login)
        soup = BeautifulSoup(login_page_response.content, 'html.parser')
        csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'}).get('value')
        login_payload = {
            'csrfmiddlewaretoken': csrf_token,
            'next': '',
            'username': username,
            'password': password
        }
        login_headers = {
            'Referer': 'https://www.screener.in/',
            'User-Agent': 'Mozilla/5.0'
        }
        login_response = session.post(url_login, data=login_payload, headers=login_headers)
        return 'Core Watchlist' in login_response.text
    except Exception as e:
        print(f"Login Error: {e}")
        return False

def fetch_data_with_retry(url, retries=10, delay=2):
    for attempt in range(retries):
        try:
            response = session.get(url)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Retry {attempt+1}/{retries} failed: {e}")
            time.sleep(delay)
    return None

def run_scraper():
    if not login_to_screener(username, password):
        print("Login failed.")
        return

    print("Login successful!")
    sh = gc.open_by_url(spreadsheet_url)
    worksheet = sh.worksheet(sheet_name)
    worksheet.batch_clear(['A1:T6000'])

    all_data = []
    blank_row = [""] * 20
    page_number = 1

    while True:
        url = url_base.format(page_number)
        response = fetch_data_with_retry(url)

        if response is None:
            break

        html_content = response.text
        dataFrames = pd.read_html(StringIO(html_content), header=0)

        if not dataFrames:
            break

        df = dataFrames[0].fillna('')
        df['Classification'] = None
        df['Hyperlink'] = None

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', class_='data-table')
        if not table:
            break

        rows = table.find('tbody').find_all('tr')

        for i, row in enumerate(rows):
            cols = row.find_all('td')
            if len(cols) > 1:
                try:
                    value = float(cols[5].text.replace(',', ''))
                    if 0.01 <= value <= 99.99:
                        classification = 1
                    elif 100 <= value <= 999.99:
                        classification = 2
                    elif 1000 <= value <= 99999.99:
                        classification = 3
                    elif value >= 100000:
                        classification = 4
                    else:
                        classification = None
                    if i > 0:
                        df.iloc[i - 1, -2] = classification
                except:
                    df.iloc[i - 1, -2] = None

                name_column = cols[1]
                link = name_column.find('a')['href'] if name_column.find('a') else ''
                full_link = f'https://www.screener.in{link}'
                hyperlink = f'=HYPERLINK("{full_link}", "{name_column.text.strip()}")'
                if i > 0:
                    df.iloc[i - 1, -1] = hyperlink

        df = df[[*df.columns[:-2], 'Classification', 'Hyperlink']]

        if 'Down  %' in df.columns:
            df['Down  %'] = df['Down  %'].apply(
                lambda x: f'-{float(x)}' if str(x).replace('.', '', 1).isdigit() else x
            )

        cleaned = df.values.tolist()
        all_data += [df.columns.tolist()] + cleaned + [blank_row]

        print(f"Page {page_number} scraped.")
        if 'Next' not in response.text:
            break
        page_number += 1

    worksheet.update(values=all_data, range_name='A1', value_input_option='USER_ENTERED')
    print("Sheet updated successfully.")

if __name__ == "__main__":
    threading.Thread(target=run_scraper).start()
    uvicorn.run(app, host="0.0.0.0", port=8000)

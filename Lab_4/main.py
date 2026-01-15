import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random

df_links = pd.read_excel('Links.xlsx')
links = df_links['Ссылка'].tolist()
results = []
output_file = 'result.xlsx'
first_write = True

for url in links:
    delay = random.uniform(16, 32)
    print(f"Ждём {delay:.1f} сек перед {url}")
    time.sleep(delay)
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Referer": "https://www.vesselfinder.com/",
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')

        if not table:
            print(f"No results table found - {url}")
            continue

        rows = table.find_all('tr')
        num_vessels = len(rows) - 1

        if num_vessels != 1:
            print(f"{url}: {num_vessels} vessels found")
            continue

        vessel_row = rows[1]
        a_tag = vessel_row.find('a')
        if not a_tag or 'href' not in a_tag.attrs:
            print(f"Skipping {url}: No details link found")
            continue

        details_url = 'https://www.vesselfinder.com' + a_tag['href']

        response_details = requests.get(details_url, headers=headers, timeout=30)
        response_details.raise_for_status()
        soup_details = BeautifulSoup(response_details.text, 'html.parser')

        name_elem = soup_details.find('h1')
        name = name_elem.text.strip() if name_elem else 'N/A'

        info_table = soup_details.find('table')
        data = {}
        if info_table:
            for tr in info_table.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) == 2:
                    key = tds[0].text.strip()
                    value = tds[1].text.strip()
                    data[key] = value

        imo = data.get('IMO', 'N/A')
        mmsi = data.get('MMSI', 'N/A')
        if imo == 'N/A' or mmsi == 'N/A':
            import re
            for val in data.values():
                if not isinstance(val, str):
                    continue
                match = re.search(r'(\d{7})\D+(\d{9})', val)
                if match:
                    imo_c = match.group(1)
                    mmsi_c = match.group(2)
                    if len(imo_c) == 7 and imo_c.isdigit():
                        imo = imo_c
                    if len(mmsi_c) == 9 and mmsi_c.isdigit():
                        mmsi = mmsi_c
                    break
        vessel_type = data.get('AIS тип', 'N/A')

        new_row = {
            'Name': name,
            'IMO': imo,
            'MMSI': mmsi,
            'Type': vessel_type,
            'Source URL': url
        }
        df_new = pd.DataFrame([new_row])

        if first_write:
            df_new.to_excel(output_file, index=False)
            first_write = False
        else:
            with pd.ExcelWriter(output_file, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
                df_new.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)
        print(f"Processed и сохранено: {url} → {name}")

    except Exception as e:
        print(f"Error processing {url}: {e}")
        continue

if results:
    df_results = pd.DataFrame(results)
    df_results.to_excel('result.xlsx', index=False)
    print("Results saved to result.xlsx")
else:
    print("No valid data found.")
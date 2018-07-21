import os
import re
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup


def download_raw_data():
    # Load data
    data = []
    with open('Data.csv', 'r', encoding='UTF-8') as f:
        for line in f:
            id, province, url = line.strip().split(',')
            if 'https://docs.google.com/spreadsheets' in url:
                url = 'https://docs.google.com/spreadsheets/u/1/d/%s/preview' % re.search(r'/d/([^/]+)/', url).group(1)
                data.append({
                    'id': id,
                    'name': province,
                    'url': url
                })
            else:
                print('Unsupported URL: ' + url)
    
    for item in data:
        response = requests.get(item['url'])
        response.raise_for_status()
        matcher = re.search(r'gid:\s+"([^"]+)"', response.content.decode('UTF-8'))
        gid = matcher.group(1)
        item['url'] = item['url'] + '/sheet?gid=' + gid

        # Save raw data
        response = requests.get(item['url'])
        response.raise_for_status()
        with open('raw/%s.html' % item['id'], 'wb') as f:
            f.write(response.content)
    
    with open('Dataset.json', 'w', encoding='UTF-8') as f:
        f.write(json.dumps(data, indent=4, ensure_ascii=False))

def parse_raw_data():
    with open('Dataset.json', 'r', encoding='UTF-8') as f:
        data = json.load(f)
    
    for item in data:
        records = []
        with open('raw/%s.html' % item['id'], 'r', encoding='UTF-8') as fp:
            soup = BeautifulSoup(fp, 'lxml')
            table_tag = soup.find('table')
            tr_tags = table_tag.find_all('tr')
            for tr_tag in tr_tags:
                td_tags = [td_tag for td_tag in tr_tag.find_all('td') if 'freezebar' not in ' '.join(td_tag.attrs.get('class', []))]
                if len(td_tags) == 0 or not re.match(r'\d{5,}', td_tags[0].text): continue
                if len(td_tags) != 17:
                    print('Error: ' + item['id'])
                    break
                records.append([td_tag.text.strip().replace(',', '.') if len(td_tag.text.strip()) else None for td_tag in td_tags])
        
        columns = ["SBD",
                    "TOÁN",
                    "VĂN",
                    "LÝ",
                    "HÓA",
                    "SINH",
                    "SỬ",
                    "ĐỊA",
                    "GDCD",
                    "ANH",
                    "KHTN",
                    "KHXH",
                    "KHỐI A",
                    "KHỐI B",
                    "KHỐI C",
                    "KHỐI D",
                    "KHỐI A1"]

        df = pd.DataFrame(records, columns=columns)
        df.to_csv('csv/%s.csv' % item['id'], index=None)

if __name__ == '__main__':
    # download_raw_data()
    parse_raw_data()

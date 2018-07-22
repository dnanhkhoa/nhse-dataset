import os
import re
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup


def download_raw_data():
    # Load data
    sources = []
    with open('sources.csv', 'r', encoding='UTF-8') as f:
        for line in f:
            id, province, url = line.strip().split(',')
            if 'https://docs.google.com/spreadsheets' in url:
                url = 'https://docs.google.com/spreadsheets/u/1/d/%s/preview' % re.search(r'/d/([^/]+)/', url).group(1)
                sources.append({
                    'id': id,
                    'name': province,
                    'url': url
                })
            else:
                print('Unsupported URL: ' + url)
    
    for item in sources:
        response = requests.get(item['url'])
        response.raise_for_status()
        item['url'] = item['url'] + '/sheet?gid=' + re.search(r'gid:\s+"([^"]+)"', response.content.decode('UTF-8')).group(1)

        # Save raw data
        response = requests.get(item['url'])
        response.raise_for_status()
        with open('raw/%s.html' % item['id'], 'wb') as f:
            f.write(response.content)
        
    with open('dataset.json', 'w', encoding='UTF-8') as f:
        f.write(json.dumps(sources, indent=4, ensure_ascii=False))

def convert_to_csv():
    with open('dataset.json', 'r', encoding='UTF-8') as f:
        sources = json.load(f)
    
    for item in sources:
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

def download_AG_data():
    # Download data
    base_url = 'http://baoangiang.com.vn/tra-cuu-diem-thi-thpt-2018.html?tensbd=&cumthi=&p=%d'
    for page_id in range(1, 818 + 1):
        url = base_url % page_id
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content.decode('UTF-8'), 'lxml')
            tbody_tag = soup.find('tbody')
            tr_tags = tbody_tag.find_all('tr')
            records = []
            for tr_tag in tr_tags:
                records.append(','.join(td_tag.text.strip().replace(',', '.') for td_tag in tr_tag.find_all('td')))
            with open('angiang/%d.txt' % page_id, 'w', encoding='UTF-8') as f:
                f.write('\n'.join(records))
        except Exception as e:
            print(e)
            print('Error: %d' % page_id)
    
    # Combine into single csv file
    records = []
    def avg(cols):
        if cols is None or any([col is None for col in cols]):
            return None
        return str(round(sum([float(col) for col in cols]) / len(cols), 2))
    
    for page_id in range(1, 818 + 1):
        with open('angiang/%d.txt' % page_id, 'r', encoding='UTF-8') as f:
            for line in f:
                p = [e if len(e.strip()) else None for e in line.strip().split(',')]
                records.append((
                    p[0],
                    p[4],
                    p[5],
                    p[7],
                    p[8],
                    p[9],
                    p[10],
                    p[11],
                    p[12],
                    p[6],
                    avg([p[7], p[8], p[9]]),
                    avg([p[10], p[11], p[12]]),
                    avg([p[4], p[7], p[8]]),
                    avg([p[4], p[8], p[9]]),
                    avg([p[5], p[10], p[11]]),
                    avg([p[4], p[5], p[6]]),
                    avg([p[4], p[6], p[7]])
                ))
    
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
    df.to_csv('52.csv', index=None)

if __name__ == '__main__':
    # download_raw_data()
    # convert_to_csv()
    # download_AG_data()
    pass

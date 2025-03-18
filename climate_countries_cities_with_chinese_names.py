import aiohttp
import asyncio
import csv
import time
from bs4 import BeautifulSoup

# NAER 外國地名譯名查詢 API
naer_url = "https://terms.naer.edu.tw/search/"

# 設定輸入 / 輸出 CSV 檔案
input_file = "climate_countries_cities.csv"
output_file = "climate_countries_cities_with_chinese_names.csv"

# 設定最大同時請求數量
MAX_CONCURRENT_REQUESTS = 10  # 可以調整（5-20）

# 查詢單個城市的中文名稱
async def fetch_chinese_name(session, city):
    params = {
        "query_term": city,
        "query_field": "title",
        "query_op": "",
        "match_type": "phrase",
        "filter_bool": "and",
        "filter_term": "'外國地名譯名'",
        "filter_field": "subcategory_1.raw",
        "filter_op": "term",
        "tabaction": "browse"
    }
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with session.get(naer_url, params=params, headers=headers, timeout=10) as response:
            if response.status != 200:
                print(f"❌ {city}: HTTP {response.status} 錯誤")
                return "N/A"

            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            # 查找包含中文名稱的 <div>
            result_div = soup.find("div", class_="td", attrs={"aria-label": "中文詞彙"})
            if result_div and result_div.a:
                chinese_name = result_div.a.text.strip()
                print(f"✅ {city} → {chinese_name}")
                return chinese_name
            else:
                print(f"❌ {city}: 無對應結果")
                return "Not Found"
    except Exception as e:
        print(f"❌ {city}: 連線錯誤 - {e}")
        return "Error"

# 批量查詢中文名稱
async def fetch_all_chinese_names(city_list):
    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # 限制最大並發數量

    async with aiohttp.ClientSession() as session:
        async def limited_fetch(city_id, country, region, city):
            async with semaphore:
                chinese_name = await fetch_chinese_name(session, city)
                results.append([city_id, country, region, city, chinese_name])

        tasks = [limited_fetch(city_id, country, region, city) for city_id, country, region, city in city_list]
        await asyncio.gather(*tasks)

    return results

# 讀取 CSV 並執行非同步查詢
async def main():
    city_list = []

    # 讀取輸入 CSV
    with open(input_file, mode="r", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        next(reader)  # 跳過標題
        city_list = [(row[0], row[1], row[2], row[3]) for row in reader]  # 取得 ID, Country, Region, City

    # 查詢中文名稱
    results = await fetch_all_chinese_names(city_list)

    # 存入 CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["ID", "Country", "Region", "City", "Chinese Name"])  # 標題
        writer.writerows(results)

    print(f"✅ 中文名稱查詢完成，存入 {output_file}")

# 執行
asyncio.run(main())

import aiohttp
import asyncio
import csv
import os
from bs4 import BeautifulSoup

# NAER 外國地名譯名查詢 API
naer_url = "https://terms.naer.edu.tw/search/"

# 設定輸入 / 輸出 CSV 檔案
input_file = "climate_countries_cities.csv"
output_file = "climate_countries_cities_with_chinese_names.csv"

# 設定最大同時請求數量 & 查詢筆數
MAX_CONCURRENT_REQUESTS = 20  # 控制並發數量，避免 API 過載
BATCH_SIZE = 100  # 每次最多查詢 100 筆，然後結束

# 讀取輸入檔案，確保所有 ID 連續
def get_all_ids_from_input():
    all_ids = set()
    city_data = {}  # 存放完整資料
    if os.path.exists(input_file):
        with open(input_file, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # 跳過標題
            for row in reader:
                city_id = int(row[0])
                all_ids.add(city_id)
                city_data[city_id] = row  # 存 ID -> row 資料
    return all_ids, city_data

# 讀取已查詢的城市中文名稱
def get_existing_data():
    existing_data = {}  # 存放已查詢過的資料
    if os.path.exists(output_file):
        with open(output_file, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # 跳過標題
            for row in reader:
                city_id = int(row[0])
                existing_data[city_id] = row  # 存整行
    return existing_data

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
    headers = {"User-Agent": "MyProject/1.0 (charliewu500@gmail.com)"}

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

# 批量查詢中文名稱（限制每次 100 筆）
async def fetch_all_chinese_names(city_list):
    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # 限制同時請求數量

    async with aiohttp.ClientSession() as session:
        async def limited_fetch(city_id, country, region, city):
            async with semaphore:
                chinese_name = await fetch_chinese_name(session, city)
                results.append([city_id, country, region, city, chinese_name])

        print(f"🔍 正在查詢 {len(city_list)} 筆城市的中文名稱...")
        tasks = [limited_fetch(city_id, country, region, city) for city_id, country, region, city in city_list]
        await asyncio.gather(*tasks)

    return results

# 讀取 CSV 並執行非同步查詢
async def main():
    all_ids, city_data = get_all_ids_from_input()  # 讀取輸入檔案的所有 ID
    existing_data = get_existing_data()  # 讀取已查詢的 ID
    city_list = []
    all_results = []  # 存放最終資料

    for city_id in sorted(all_ids):
        country, region, city = city_data[city_id][1], city_data[city_id][2], city_data[city_id][3]

        # 如果 `City == "-"`，則填空，不查詢
        if city == "-":
            all_results.append([city_id, country, region, city, ""])
            continue  # 跳過查詢

        if city_id in existing_data:
            # 若已存在，檢查是否需要重新查詢
            row = existing_data[city_id]
            if row[4] == "N/A":  # 若中文名稱是 "N/A"，重新查詢
                city_list.append((city_id, country, region, city))
            all_results.append(row)  # 加入現有資料
        else:
            # 需要查詢的城市
            city_list.append((city_id, country, region, city))

    # **限制每次查詢 100 筆**
    if city_list:
        city_list = city_list[:BATCH_SIZE]  # 只取前 100 筆
        print(f"🔍 這次執行最多查詢 {len(city_list)} 筆")
        results = await fetch_all_chinese_names(city_list)
        all_results.extend(results)  # 加入新的查詢結果
    else:
        print("✅ 所有城市的中文名稱都已查詢完成，無需繼續。")

    # **確保 ID 重新排序**
    all_results.sort(key=lambda x: int(x[0]))

    # 存入 CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["ID", "Country", "Region", "City", "Chinese Name"])  # 寫入標題
        writer.writerows(all_results)

    print(f"✅ 中文名稱查詢完成，存入 {output_file}，這次執行結束！")

# 執行
asyncio.run(main())

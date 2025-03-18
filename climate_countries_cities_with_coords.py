import aiohttp
import asyncio
import csv
import time

# OpenStreetMap Nominatim API
nominatim_url = "https://nominatim.openstreetmap.org/search"

# 設定輸入 / 輸出 CSV 檔案
input_file = "climate_countries_cities.csv"
output_file = "climate_countries_cities_with_coords.csv"

# 設定最大同時請求數量
MAX_CONCURRENT_REQUESTS = 10  # 可以調整（5-20）

# 查詢單個城市的經緯度
async def fetch_coordinates(session, city, country):
    params = {"q": f"{city}, {country}", "format": "json", "limit": 1}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with session.get(nominatim_url, params=params, headers=headers, timeout=10) as response:
            data = await response.json()
            if data:
                lat, lon = data[0]["lat"], data[0]["lon"]
                print(f"{city}, {country} → {lat}, {lon} ✅")
                return lat, lon
            else:
                print(f"{city}, {country} ❌ 找不到經緯度")
                return "N/A", "N/A"
    except Exception as e:
        print(f"{city}, {country} ❌ 連線錯誤 - {e}")
        return "N/A", "N/A"

# 批量查詢經緯度
async def fetch_all_coordinates(city_list):
    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # 限制最大並發數量

    async with aiohttp.ClientSession() as session:
        async def limited_fetch(city, country, city_id):
            async with semaphore:
                lat, lon = await fetch_coordinates(session, city, country)
                results.append([city_id, city, country, lat, lon])

        tasks = [limited_fetch(city, country, city_id) for city_id, city, country in city_list]
        await asyncio.gather(*tasks)

    return results

# 讀取 CSV 並執行非同步查詢
async def main():
    city_list = []

    # 讀取輸入 CSV
    with open(input_file, mode="r", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        next(reader)  # 跳過標題
        city_list = [(row[0], row[1], row[2]) for row in reader]  # 取得 ID, City, Country

    # 查詢經緯度
    results = await fetch_all_coordinates(city_list)

    # 存入 CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["ID", "City", "Country", "Latitude", "Longitude"])  # 標題
        writer.writerows(results)

    print(f"✅ 經緯度查詢完成，存入 {output_file}")

# 執行
asyncio.run(main())

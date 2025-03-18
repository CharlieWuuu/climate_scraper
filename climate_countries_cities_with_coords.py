import aiohttp
import asyncio
import csv
import os

# OpenStreetMap Nominatim API
nominatim_url = "https://nominatim.openstreetmap.org/search"

# 設定輸入 / 輸出 CSV 檔案
input_file = "climate_countries_cities.csv"
output_file = "climate_countries_cities_with_coords.csv"

# 設定最大同時請求數量 & 查詢筆數
MAX_CONCURRENT_REQUESTS = 3  # 控制並發數量，避免 API 過載
BATCH_SIZE = 30  # 每次最多查詢 30 筆，然後結束

# **讀取 climate_countries_cities.csv**
def get_all_city_data():
    city_data = {}  # 存放完整資料
    if os.path.exists(input_file):
        with open(input_file, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader, None)  # 跳過標題
            for row in reader:
                city_id = int(row[0])
                city_data[city_id] = row[1:4]  # 只存 `Country`、`Region`、`City`
    return city_data

# **讀取 climate_countries_cities_with_coords.csv**
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

# **查詢單個城市的經緯度**
async def fetch_coordinates(session, country, region, city):
    params = {"q": f"{country}, {region}, {city}", "format": "json", "limit": 1}
    headers = {"User-Agent": "MyProject/1.0 (charliewu500@gmail.com)"}

    try:
        async with session.get(nominatim_url, params=params, headers=headers, timeout=10) as response:
            if response.status != 200:
                print(f"❌ {country}, {region}, {city}: HTTP {response.status} 錯誤")
                return "N/A", "N/A"

            data = await response.json()
            if data:
                lat, lon = data[0]["lat"], data[0]["lon"]
                print(f"✅ {country}, {region}, {city} → {lat}, {lon}")
                return lat, lon
            else:
                print(f"❌ {country}, {region}, {city}: 找不到經緯度")
                return "N/A", "N/A"
    except Exception as e:
        print(f"❌ {country}, {region}, {city}: 連線錯誤 - {e}")
        return "N/A", "N/A"

# **批量查詢經緯度**
async def fetch_all_coordinates(city_list):
    results = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # 限制同時請求數量

    async with aiohttp.ClientSession() as session:
        async def limited_fetch(city_id, country, region, city):
            async with semaphore:
                lat, lon = await fetch_coordinates(session, country, region, city)
                results.append([city_id, country, region, city, lat, lon])
                await asyncio.sleep(1.5)  # 增加請求間隔

        print(f"🔍 正在查詢 {len(city_list)} 筆城市的經緯度...")
        tasks = [limited_fetch(city_id, country, region, city) for city_id, country, region, city in city_list]
        await asyncio.gather(*tasks)

    return results

# **讀取 CSV 並執行非同步查詢**
async def main():
    city_data = get_all_city_data()  # 讀取 climate_countries_cities.csv
    existing_data = get_existing_data()  # 讀取 climate_countries_cities_with_coords.csv
    city_list = []
    all_results = []  # 存放最終資料

    for city_id, (country, region, city) in sorted(city_data.items()):
        # **如果 `Country`、`Region`、`City` 都是 `"-"`，則不查詢**
        if country == "-" and region == "-" and city == "-":
            all_results.append([city_id, country, region, city, "N/A", "N/A"])
            continue  # 跳過查詢

        if city_id in existing_data:
            row = existing_data[city_id]
            old_country, old_region, old_city, lat, lon = row[1:6]

            # **如果資料不同，更新 Country、Region、City，並重新查詢**
            # if (old_country != country or old_region != region or old_city != city) or (lat == "N/A" or lon == "N/A"):
                # print(f"🔄 {city_id}: {old_country}, {old_region}, {old_city} → {country}, {region}, {city} (重新查詢)")
                # city_list.append((city_id, country, region, city))
            # else:
            all_results.append(row)  # 保留原資料
        else:
            # **新的 ID 需要查詢**
            city_list.append((city_id, country, region, city))

    # **限制每次查詢 30 筆**
    if city_list:
        city_list = city_list[:BATCH_SIZE]  # 只取前 30 筆
        print(f"🔍 這次執行最多查詢 {len(city_list)} 筆")
        results = await fetch_all_coordinates(city_list)
        all_results.extend(results)  # 加入新的查詢結果
    else:
        print("✅ 所有城市的經緯度都已查詢完成，無需繼續。")

    # **確保 ID 重新排序**
    all_results.sort(key=lambda x: int(x[0]))

    # **存入 CSV**
    with open(output_file, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["ID", "Country", "Region", "City", "Latitude", "Longitude"])  # 寫入標題
        writer.writerows(all_results)

    print(f"✅ 經緯度查詢完成，存入 {output_file}，這次執行結束！")

# **執行**
asyncio.run(main())

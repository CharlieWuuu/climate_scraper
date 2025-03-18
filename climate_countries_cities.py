import aiohttp
import asyncio
import csv
import re
import os
from bs4 import BeautifulSoup

# 設定爬取的網址s
base_url = "https://en.climate-data.org/a/a/a/a-{}/"
output_file = "climate_countries_cities.csv"
MAX_CONCURRENT_REQUESTS = 20  # 限制同時請求數量
BATCH_SIZE = 100  # 每次最多新增 30 筆資料
months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# **讀取 CSV，找出不完整的資料並確保 ID 連續**
def get_incomplete_and_missing_ids():
    existing_ids = set()
    incomplete_ids = []
    existing_data = []

    if os.path.exists(output_file):
        with open(output_file, mode="r", encoding="utf-8") as infile:
            reader = csv.reader(infile)
            headers = next(reader)  # 讀取標題
            for row in reader:
                if not row:
                    continue

                city_id = int(row[0])
                country, region, city = row[1:4]
                climate_type, avg_temp_c, annual_rain_mm = row[4:7]

                existing_ids.add(city_id)

                # **如果資料不完整，加入重新爬取清單**
                if country != "-" and city == "-":
                    incomplete_ids.append(city_id)
                else:
                    existing_data.append(row)

    # **確保 ID 連續**
    max_id = max(existing_ids) if existing_ids else 0
    missing_ids = [i for i in range(1, max_id + BATCH_SIZE + 1) if i not in existing_ids]

    print(f"🔍 找到 {len(incomplete_ids)} 筆不完整的資料")
    print(f"📌 需要新增 {len(missing_ids[:BATCH_SIZE])} 筆新資料")

    return incomplete_ids, missing_ids[:BATCH_SIZE], headers, existing_data

# **爬取單個城市頁面**
async def fetch_city(session, city_id):
    url = base_url.format(city_id)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status == 404:
                print(f"ID {city_id}: 404 Not Found，存空值")
                return [city_id, "-", "-", "-", "-", "-", "-", "-", "-", "-", *["-"] * 12 * 7]

            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            # **解析地理資訊**
            breadcrumbs = soup.select("ol[itemtype='http://schema.org/BreadcrumbList'] li span[itemprop='name']")
            location_hierarchy = [bc.text.strip() for bc in breadcrumbs[1:]] if breadcrumbs else []

            country, region, city = "-", "-", "-"
            if len(location_hierarchy) == 3:
                country, region, city = location_hierarchy
            elif len(location_hierarchy) == 2:
                country, region, city = location_hierarchy[0], "-", location_hierarchy[1]
            elif len(location_hierarchy) == 1:
                country, region, city = location_hierarchy[0], "-", "-"

            print(f"ID {city_id}: {country}, {region}, {city} ✅")

            # **解析氣候數據**
            article = soup.select_one("#article")
            if article:
                text = article.get_text()

                # **抓取 Köppen-Geiger 氣候分類**
                climate_match = re.search(r"\b(Af|Am|As|Aw|BWh|BWk|BSh|BSk|Cfa|Cfb|Cfc|Cwa|Cwb|Cwc|Csa|Csb|Csc|Dfa|Dfb|Dfc|Dfd|Dwa|Dwb|Dwc|Dwd|Dsa|Dsb|Dsc|Dsd|ET|EF)\b", text)
                climate_type = climate_match.group(1).strip() if climate_match else "-"

                # **抓取年均溫、降雨量**
                temp_match = re.search(r"temperature.*? ([\d.]+) °C", text)
                avg_temp_c = temp_match.group(1) if temp_match else "-"

                rain_match = re.search(r"rainfall.*? ([\d.]+) mm", text)
                annual_rain_mm = rain_match.group(1) if rain_match else "-"

                # **抓取所屬半球、夏季月份、最佳旅遊時間**
                hemisphere_match = re.search(r"([Nn]orthern|[Ss]outhern) [Hh]emisphere", text)
                hemisphere = hemisphere_match.group(1).capitalize() if hemisphere_match else "-"

                summer_match = re.search(r"Summer.*? ([A-Za-z, ]+)", text)
                summer_months = summer_match.group(1).strip() if summer_match else "-"

                visit_match = re.search(r"best time to visit is ([A-Za-z, ]+)", text)
                best_visit_time = visit_match.group(1).strip() if visit_match else "-"

                # **解析 monthly weather data**
                weather_table = soup.select_one("#weather_table tbody")
                monthly_data = {}
                if weather_table:
                    rows = weather_table.find_all("tr")
                    for row in rows:
                        cols = row.find_all("td")
                        if cols:
                            key = cols[0].text.strip()
                            values = [re.sub(r'[^\d.-]', '', col.text.strip().splitlines()[0]) for col in cols[1:]]
                            monthly_data[key] = values

                # **轉換 monthly_data**
                weather_data_expanded = []
                for key in ["Avg. Temperature °C (°F)", "Min. Temperature °C (°F)", "Max. Temperature °C (°F)",
                            "Precipitation / Rainfall mm (in)", "Humidity(%)", "Rainy days (d)", "avg. Sun hours (hours)"]:
                    values = monthly_data.get(key, ["-"] * 12)
                    weather_data_expanded.extend(values)

                return [city_id, country, region, city, climate_type, avg_temp_c,
                        annual_rain_mm, hemisphere, summer_months, best_visit_time, *weather_data_expanded]

            print(f"ID {city_id}: ❌ 找不到數據，存空值")
            return [city_id, "-", "-", "-", "-", "-", "-", "-", "-", "-", *["-"] * 12 * 7]

    except Exception as e:
        print(f"ID {city_id}: ❌ 連線錯誤 - {e}")
        return [city_id, "-", "-", "-", "-", "-", "-", "-", "-", "-", *["-"] * 12 * 7]

# **執行爬取**
async def scrape_cities():
    incomplete_ids, new_ids, headers, existing_data = get_incomplete_and_missing_ids()

    ids_to_fetch = incomplete_ids + new_ids  # **合併需要重新爬取和新增的 ID**
    if not ids_to_fetch:
        print("✅ 沒有需要重新抓取的資料")
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        async def limited_fetch(city_id):
            async with semaphore:
                return await fetch_city(session, city_id)

        tasks = [limited_fetch(city_id) for city_id in ids_to_fetch]
        new_results = await asyncio.gather(*tasks)

    # **合併舊資料與新資料，並按照 ID 排序**
    combined_data = existing_data + new_results
    combined_data.sort(key=lambda x: int(x[0]))  # **確保 ID 連續排序**

    # **覆寫 CSV**
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(combined_data)

    print(f"✅ 爬取完成，資料已更新並重新排序至 {output_file}")

# **執行爬蟲**
asyncio.run(scrape_cities())

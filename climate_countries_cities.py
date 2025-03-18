import aiohttp
import asyncio
import csv
import re
from bs4 import BeautifulSoup
import time

# 設定爬取的 ID 範圍
start_id = 1
end_id = 30  # 這是已知的最大 ID
base_url = "https://en.climate-data.org/a/a/a/a-{}/"

# 設定輸出 CSV
output_file = "climate_countries_cities.csv"

# 設定最大同時請求數量
MAX_CONCURRENT_REQUESTS = 20  # 建議 5-20，可調整

# 爬取單個城市頁面
async def fetch_city(session, city_id):
    url = base_url.format(city_id)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status == 404:
                print(f"ID {city_id}: 404 Not Found，跳過")
                return None  # 404 直接跳過

            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            # 找 h1 標籤（城市名稱 + 國家）
            h1_tag = soup.find("h1")
            if h1_tag:
                full_name = h1_tag.text.strip()
                if "Climate" in full_name and "(" in full_name and ")" in full_name:
                    city = full_name.split(" Climate")[0].strip()
                    country = full_name.split("(")[-1].split(")")[0].strip()
                    print(f"ID {city_id}: {city}, {country} ✅")

                    # 解析文章中的氣候數據
                    article = soup.select_one("#article")
                    if article:
                        text = article.get_text()

                        # 抓取 Köppen-Geiger 氣候分類（兩種寫法）
                        climate_match = re.search(r"\b(Af|Am|As|Aw|BWh|BWk|BSh|BSk|Cfa|Cfb|Cfc|Cwa|Cwb|Cwc|Csa|Csb|Csc|Dfa|Dfb|Dfc|Dfd|Dwa|Dwb|Dwc|Dwd|Dsa|Dsb|Dsc|Dsd|ET|EF)\b", text)
                        climate_type = climate_match.group(1).strip() if climate_match else "-"

                        # 抓取年均溫
                        temp_match = re.search(r"temperature.*? ([\d.]+) °C \| ([\d.]+) °F", text)
                        avg_temp_c = temp_match.group(1) if temp_match else "-"
                        avg_temp_f = temp_match.group(2) if temp_match else "-"

                        # 抓取年降雨量
                        rain_match = re.search(r"rainfall.*? ([\d.]+) mm \| ([\d.]+) inch", text)
                        annual_rain_mm = rain_match.group(1) if rain_match else "-"
                        annual_rain_inch = rain_match.group(2) if rain_match else "-"

                        # 抓取所屬半球
                        hemisphere_match = re.search(r"([Nn]orthern|[Ss]outhern) [Hh]emisphere", text)
                        hemisphere = hemisphere_match.group(1).capitalize() if hemisphere_match else "-"

                        # 抓取夏季月份
                        summer_match = re.search(r"Summer.*? ([A-Za-z, ]+)", text)
                        summer_months = summer_match.group(1).strip() if summer_match else "-"

                        # 抓取最佳旅遊時間（如果有的話）
                        visit_match = re.search(r"best time to visit is ([A-Za-z, ]+)", text)
                        best_visit_time = visit_match.group(1).strip() if visit_match else "-"

                        # 抓取 monthly weather data
                        weather_table = soup.select_one("#weather_table tbody")
                        if weather_table:
                            rows = weather_table.find_all("tr")
                            monthly_data = {}
                            for row in rows:
                                cols = row.find_all("td")
                                if cols:
                                    key = cols[0].text.strip()
                                    values = [col.text.strip().split(" ")[0] for col in cols[1:]]
                                    monthly_data[key] = values
                        else:
                            monthly_data = {}
                        return [city_id, city, country, climate_type, avg_temp_c, avg_temp_f, annual_rain_mm, annual_rain_inch, hemisphere, summer_months, best_visit_time, monthly_data]
                    else:
                        return [city_id, city, country, "-", "-", "-", "-", "-", "-", "-", "-", {}]
                else:
                    print(f"ID {city_id}: ❌ 格式錯誤")
                    return None
            else:
                print(f"ID {city_id}: ❌ 找不到 h1")
                return None

    except Exception as e:
        print(f"ID {city_id}: ❌ 連線錯誤 - {e}")
        return None

# 控制並發數量（批量請求）
async def scrape_all_cities():
    tasks = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)  # 限制同時請求數量

    async with aiohttp.ClientSession() as session:
        async def limited_fetch(city_id):
            async with semaphore:
                return await fetch_city(session, city_id)

        tasks = [limited_fetch(city_id) for city_id in range(start_id, end_id + 1)]
        results = await asyncio.gather(*tasks)

    # 移除 None（因為 404 或錯誤可能會返回 None）
    results = [row for row in results if row is not None]

    # 存入 CSV
    with open(output_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["ID", "City", "Country", "Climate Type", "Avg Temp (°C)", "Avg Temp (°F)", "Annual Rainfall (mm)", "Annual Rainfall (inch)", "Hemisphere", "Summer Months", "Best Visit Time", "Monthly Weather Data"])  # CSV 標題
        writer.writerows(results)

    print(f"✅ 爬取完成，資料已存入 {output_file}")

# 執行爬蟲
asyncio.run(scrape_all_cities())

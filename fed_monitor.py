import os
import requests
import pandas as pd
import io
import time
from datetime import datetime

# 获取环境变量
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

def fetch_and_push():
    print("-" * 50)
    print(f"云端运行强制修正版: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 直接定义完整的 URL，不使用任何拼接逻辑
    tasks = [
        {"id": "DPSACBW027SBOG", "name": "01. 银行存款总额 (SA)", "url": "https://fred.stlouisfed.org/series/DPSACBW027SBOG/downloaddata/DPSACBW027SBOG.csv"},
        {"id": "DPNSBW027SBOG", "name": "02. 银行存款总额 (NSA)", "url": "https://fred.stlouisfed.org/series/DPNSBW027SBOG/downloaddata/DPNSBW027SBOG.csv"},
        {"id": "DLCBW027SBOG",  "name": "03. 大型银行 (SA)", "url": "https://fred.stlouisfed.org/series/DLCBW027SBOG/downloaddata/DLCBW027SBOG.csv"},
        {"id": "DSCBW027SBOG",  "name": "04. 小型银行 (SA)", "url": "https://fred.stlouisfed.org/series/DSCBW027SBOG/downloaddata/DSCBW027SBOG.csv"},
        {"id": "DFXBW027SBOG",  "name": "05. 外国银行 (SA)", "url": "https://fred.stlouisfed.org/series/DFXBW027SBOG/downloaddata/DFXBW027SBOG.csv"},
        {"id": "WM2NS",         "name": "06. M2 货币供应量 (NSA)", "url": "https://fred.stlouisfed.org/series/WM2NS/downloaddata/WM2NS.csv"}
    ]

    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    # 简单的浏览器伪装
    u_agent = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    for item in tasks:
        print(f"正在抓取: {item['name']}...", end=" ")
        try:
            # 1. 抓取 FRED 数据
            resp = requests.get(item['url'], headers=u_agent, timeout=30)
            if resp.status_code != 200:
                print(f"❌ 下载失败 ({resp.status_code})")
                continue
            
            # 2. 解析 CSV
            df = pd.read_csv(io.StringIO(resp.text))
            latest_val = float(df.iloc[-1]['VALUE'])
            prev_val = float(df.iloc[-2]['VALUE'])
            change = latest_val - prev_val
            data_date = str(df.iloc[-1]['DATE'])

            trend = "🟢 增加" if change > 0 else ("🔴 减少" if change < 0 else "⚪ 持平")

            # 3. 构造 Notion 数据包
            payload = {
                "parent": {"database_id": DATABASE_ID},
                "properties": {
                    "指标名称": {"title": [{"text": {"content": item['name']}}]},
                    "本周余额(十亿)": {"number": round(latest_val, 1)},
                    "周变化量": {"number": round(change, 1)},
                    "趋势": {"rich_text": [{"text": {"content": trend}}]},
                    "更新日期": {"date": {"start": data_date}}
                }
            }
            
            r = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload, timeout=15)
            print("✅ 成功" if r.status_code == 200 else f"❌ 推送失败 ({r.status_code})")
            
            time.sleep(1) # 防止请求过快
            
        except Exception as e:
            print(f"❌ 运行报错: {e}")

    print("-" * 50)
    print("任务执行完毕！")

if __name__ == "__main__":
    fetch_and_push()

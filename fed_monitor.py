import os
import requests
import pandas as pd
import io
import time
from datetime import datetime

# 核心逻辑：从 GitHub 的 Secrets 安全读取配置
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

def fetch_and_push():
    print("-" * 50)
    print(f"云端运行开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    series_map = {
        'DPSACBW027SBOG': '01. 银行存款总额 (SA)',
        'DPNSBW027SBOG': '02. 银行存款总额 (NSA)',
        'DLCBW027SBOG':  '03. 大型银行 (SA)',
        'DSCBW027SBOG':  '04. 小型银行 (SA)',
        'DFXBW027SBOG':  '05. 外国银行 (SA)',
        'WM2NS':         '06. M2 货币供应量 (NSA)'
    }

    notion_headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for s_id, s_name in series_map.items():
        print(f"正在抓取: {s_name}...", end=" ")
        try:
            url = f"https://fred.stlouisfed.org/series/{s_id}/downloaddata/{s_id}.csv"
            resp = requests.get(url, headers=browser_headers, timeout=30)
            
            if resp.status_code == 200:
                df = pd.read_csv(io.StringIO(resp.text))
                latest_val = float(df.iloc[-1]['VALUE'])
                prev_val = float(df.iloc[-2]['VALUE'])
                change = latest_val - prev_val
                data_date = df.iloc[-1]['DATE']

                trend = "🟢 增加" if change > 0 else ("🔴 减少" if change < 0 else "⚪ 持平")

                payload = {
                    "parent": {"database_id": DATABASE_ID},
                    "properties": {
                        "指标名称": {"title": [{"text": {"content": s_name}}]},
                        "本周余额(十亿)": {"number": round(latest_val, 1)},
                        "周变化量": {"number": round(change, 1)},
                        "趋势": {"rich_text": [{"text": {"content": trend}}]},
                        "更新日期": {"date": {"start": data_date}}
                    }
                }
                requests.post("https://api.notion.com/v1/pages", headers=notion_headers, json=payload, timeout=15)
                print("✅ 成功")
            else:
                print(f"❌ 失败 (HTTP {resp.status_code})")
            time.sleep(1)
        except Exception as e:
            print(f"❌ 错误: {e}")

if __name__ == "__main__":
    fetch_and_push()
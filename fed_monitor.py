import os
import requests
import pandas as pd
import io
import time
from datetime import datetime

# 从 GitHub Secrets 安全读取
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

def fetch_and_push():
    print("-" * 50)
    print(f"云端运行开始: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 明确指标 ID 和名称
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

    # 模拟浏览器 User-Agent
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for s_id, s_name in series_map.items():
        print(f"正在处理: {s_name}...", end=" ")
        try:
            # 纯净的 URL 构造，不再使用变量拼接，防止 Errno -3
            target_url = f"https://fred.stlouisfed.org/series/{s_id}/downloaddata/{s_id}.csv"
            
            resp = requests.get(target_url, headers=browser_headers, timeout=30)
            
            if resp.status_code == 200:
                df = pd.read_csv(io.StringIO(resp.text))
                if df.empty or len(df) < 2:
                    print("❌ 数据行数不足")
                    continue
                
                # FRED CSV 的列名通常是 'DATE' 和 'VALUE'
                latest_val = float(df.iloc[-1]['VALUE'])
                prev_val = float(df.iloc[-2]['VALUE'])
                change = latest_val - prev_val
                data_date = str(df.iloc[-1]['DATE'])

                trend = "🟢 增加" if change > 0 else ("🔴 减少" if change < 0 else "⚪ 持平")

                # 构造推送 Payload
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
                
                notion_resp = requests.post("https://api.notion.com/v1/pages", headers=notion_headers, json=payload, timeout=15)
                
                if notion_resp.status_code == 200:
                    print("✅ 成功")
                else:
                    print(f"❌ Notion 报错: {notion_resp.status_code}")
            else:
                print(f"❌ FRED 下载失败: {resp.status_code}")
            
            time.sleep(1) # 频率保护
            
        except Exception as e:
            print(f"❌ 运行异常: {e}")

    print("-" * 50)
    print("任务执行完毕！")

if __name__ == "__main__":
    fetch_and_push()

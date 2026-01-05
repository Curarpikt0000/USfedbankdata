import pandas as pd
from fredapi import Fred
from datetime import datetime, timedelta
import requests
import time
import os
import sys

# ================= 安全配置 (从GitHub Secrets读取) =================
FRED_API_KEY = os.getenv('FRED_API_KEY')
NOTION_TOKEN = os.getenv('NOTION_TOKEN')
DATABASE_ID = os.getenv('NOTION_DATABASE_ID')

# 检查是否获取到了 Key，如果没有则报错停止
if not FRED_API_KEY or not NOTION_TOKEN or not DATABASE_ID:
    print("❌ Error: Environment variables are missing!")
    sys.exit(1)
# ================================================================

fred = Fred(api_key=FRED_API_KEY)

indicators = {
    'M2': {'sa': 'M2SL', 'nsa': 'M2NS', 'name': '06.M2供应', 'detail': 'M2'},
    'Large_Banks': {'sa': 'DPSLCBW027SBOG', 'nsa': 'DPSLCBW027NBOG', 'name': '大型银行', 'detail': '大型银行'},
    'Small_Banks': {'sa': 'DPSSCBW027SBOG', 'nsa': 'DPSSCBW027NBOG', 'name': '小型银行', 'detail': '小型银行'},
    'Foreign_Banks': {'sa': 'DPSFRIW027SBOG', 'nsa': 'DPSFRIW027NBOG', 'name': '外国银行', 'detail': '外国银行'},
    'All_Banks': {'sa': 'DPSACBW027SBOG', 'nsa': 'DPSACBW027NBOG', 'name': '银行整体', 'detail': '银行整体'}
}

def get_fred_data(series_id, start_date):
    try:
        return fred.get_series(series_id, observation_start=start_date)
    except Exception:
        return pd.Series()

def push_to_notion(data_row):
    url = "https://api.notion.com/v1/pages"
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    payload = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            "指标名称": { "title": [{"text": {"content": data_row['指标名称']}}] },
            "Detail指标": { "select": {"name": data_row['Detail指标']} },
            "类型": { "select": {"name": data_row['类型']} },
            "本周余额": { "number": data_row['本周余额'] },
            "周变化量": { "number": data_row['周变化量'] },
            "Seasonal Wedge": { "number": data_row['Seasonal Wedge'] },
            "趋势": { "select": {"name": data_row['趋势']} },
            "更新日期": { "date": {"start": data_row['更新日期']} }
        }
    }
    try:
        resp = requests.post(url, json=payload, headers=headers)
        if resp.status_code == 200:
            print(f"✅ Uploaded: {data_row['更新日期']} - {data_row['指标名称']}")
        else:
            print(f"⚠️ Fail: {resp.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    # 🔴 只回溯 14 天 (确保覆盖最新的周五)
    days_back = 14
    start_date = datetime.today() - timedelta(days=days_back)
    
    print(f"🚀 Weekly Update: Fetching data since {start_date.date()}...")
    
    processed_rows = []
    
    for key, info in indicators.items():
        sa = get_fred_data(info['sa'], start_date)
        nsa = get_fred_data(info['nsa'], start_date)
        
        if sa.empty or nsa.empty: continue
            
        df = pd.DataFrame({'SA': sa, 'NSA': nsa}).ffill().dropna()

        for date, row in df.iterrows():
            prev_idx = df.index.get_loc(date) - 1
            if prev_idx < 0: continue 

            val_sa, val_nsa = row['SA'], row['NSA']
            change_sa = val_sa - df.iloc[prev_idx]['SA']
            change_nsa = val_nsa - df.iloc[prev_idx]['NSA']
            
            # SA
            processed_rows.append({
                '指标名称': info['name'], 'Detail指标': info['detail'], '类型': 'SA',
                '本周余额': round(val_sa, 2), '周变化量': round(change_sa, 2), 'Seasonal Wedge': 0,
                '趋势': '🟢增加' if change_sa > 0 else '🔴减少', '更新日期': date.strftime('%Y-%m-%d')
            })
            # NSA
            processed_rows.append({
                '指标名称': info['name'], 'Detail指标': info['detail'], '类型': 'NSA',
                '本周余额': round(val_nsa, 2), '周变化量': round(change_nsa, 2), 
                'Seasonal Wedge': round(val_nsa - val_sa, 2),
                '趋势': '🟢增加' if change_nsa > 0 else '🔴减少', '更新日期': date.strftime('%Y-%m-%d')
            })

    # 只上传最近的一期数据 (避免把14天内的旧数据重复上传)
    # 逻辑：找出列表里日期最新的那一天，只上传那一天的数据
    if processed_rows:
        latest_date = max(row['更新日期'] for row in processed_rows)
        latest_rows = [r for r in processed_rows if r['更新日期'] == latest_date]
        
        print(f"📊 Found latest data for {latest_date}. Uploading {len(latest_rows)} rows...")
        for row in latest_rows:
            push_to_notion(row)
            time.sleep(0.3)
    else:
        print("💤 No new data found in the last 14 days.")

if __name__ == "__main__":
    main()

import os, requests, pandas as pd, io, time

def fetch():
    # --- 防伪标记：如果在日志里看不到这句话，说明代码没更新成功 ---
    print("🚀 DEBUG: 正在运行 V2026 新版代码...")
    
    # 读取密钥
    tok = os.getenv("NOTION_TOKEN")
    dbid = os.getenv("NOTION_DATABASE_ID")
    
    tasks = [
        ("01.总存款(SA)", "https://fred.stlouisfed.org/series/DPSACBW027SBOG/downloaddata/DPSACBW027SBOG.csv"),
        ("02.总存款(NSA)", "https://fred.stlouisfed.org/series/DPNSBW027SBOG/downloaddata/DPNSBW027SBOG.csv"),
        ("03.大型银行", "https://fred.stlouisfed.org/series/DLCBW027SBOG/downloaddata/DLCBW027SBOG.csv"),
        ("04.小型银行", "https://fred.stlouisfed.org/series/DSCBW027SBOG/downloaddata/DSCBW027SBOG.csv"),
        ("05.外国银行", "https://fred.stlouisfed.org/series/DFXBW027SBOG/downloaddata/DFXBW027SBOG.csv"),
        ("06.M2供应", "https://fred.stlouisfed.org/series/WM2NS/downloaddata/WM2NS.csv")
    ]

    n_h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

    for name, target_url in tasks:
        # 打印出正在请求的真实 URL，确保证据确凿
        print(f"🔗 正在请求: {target_url}")
        
        try:
            r = requests.get(target_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text))
                val, prev = float(df.iloc[-1]['VALUE']), float(df.iloc[-2]['VALUE'])
                change, date = val - prev, str(df.iloc[-1]['DATE'])
                
                payload = {
                    "parent": {"database_id": dbid},
                    "properties": {
                        "指标名称": {"title": [{"text": {"content": name}}]},
                        "本周余额(十亿)": {"number": round(val, 1)},
                        "周变化量": {"number": round(change, 1)},
                        "趋势": {"rich_text": [{"text": {"content": "🟢增加" if change > 0 else "🔴减少"}}]},
                        "更新日期": {"date": {"start": date}}
                    }
                }
                res = requests.post("https://api.notion.com/v1/pages", headers=n_h, json=payload)
                print(f"✅ 成功: {name} (状态码: {res.status_code})")
            else:
                print(f"❌ 下载失败 {name}: {r.status_code}")
            time.sleep(1)
        except Exception as e:
            print(f"❌ 严重报错 {name}: {e}")

if __name__ == "__main__":
    fetch()

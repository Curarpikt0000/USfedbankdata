import os, requests, pandas as pd, io, time

def fetch():
    # 强制读取 Secrets
    tok = os.getenv("NOTION_TOKEN")
    dbid = os.getenv("NOTION_DATABASE_ID")
    
    # 任务配置：指标名 与 绝对物理链接
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
        print(f"Starting: {name}")
        try:
            # 这里的 target_url 是绝对字符串，不再经过任何拼接
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
                print(f"Success: {name} | Notion Status: {res.status_code}")
            else:
                print(f"Failed to download {name}: {r.status_code}")
            time.sleep(1)
        except Exception as e:
            print(f"Error on {name}: {e}")

if __name__ == "__main__":
    fetch()

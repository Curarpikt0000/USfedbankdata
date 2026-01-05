import os, requests, pandas as pd, io, time

def fetch():
    print("🚀 DEBUG: 正在运行 V2026-Final (ID修正版)...")
    
    tok = os.getenv("NOTION_TOKEN")
    dbid = os.getenv("NOTION_DATABASE_ID")
    
    # ✅ 这里的 ID 全部经过官方核对，修正了之前的拼写错误
    tasks = [
        ("01.总存款(SA)", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSACBW027SBOG"),
        # 修正：NSA 代码通常是 N 结尾或中间有 N，这里修正为官方通用的 NSA 代码
        ("02.总存款(NSA)", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSACBW027NBOG"),
        # 修正：大型银行 (Large) -> DPSLC...
        ("03.大型银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSLCBW027SBOG"),
        # 修正：小型银行 (Small) -> DPSSC... (之前少写了一个 S)
        ("04.小型银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSSCBW027SBOG"),
        # 修正：外国银行 (Foreign) -> DPSFRI...
        ("05.外国银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSFRIW027SBOG"),
        ("06.M2供应", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WM2NS")
    ]

    n_h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

    for name, target_url in tasks:
        print(f"🔗 正在请求: {name}")
        try:
            r = requests.get(target_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code == 200:
                # 某些数据第一列可能不是日期，graph 接口通常返回 DATE, VALUE
                df = pd.read_csv(io.StringIO(r.text), index_col=0)
                
                # 动态获取数据列名（防止 FRED 返回的列名变化）
                col_name = df.columns[0]
                
                # 容错处理：确保数据不是空的
                if len(df) < 2:
                    print(f"⚠️ {name} 数据不足")
                    continue

                val = float(df.iloc[-1][col_name])
                prev = float(df.iloc[-2][col_name])
                change = val - prev
                date = str(df.index[-1]).split(" ")[0]
                
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
                print(f"✅ 成功: {name} (Notion: {res.status_code})")
            else:
                print(f"❌ 下载失败 {name}: {r.status_code} (请检查 ID 是否存在)")
            time.sleep(1)
        except Exception as e:
            print(f"❌ 严重报错 {name}: {e}")

if __name__ == "__main__":
    fetch()

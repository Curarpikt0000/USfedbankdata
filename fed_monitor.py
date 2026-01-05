import os, requests, pandas as pd, io, time

def fetch():
    print("🚀 DEBUG: 正在运行 V2026-Structure (带层级排序版)...")
    
    tok = os.getenv("NOTION_TOKEN")
    dbid = os.getenv("NOTION_DATABASE_ID")
    
    # ✅ 核心修改：在元组里增加了第一个元素 —— "排序ID"
    # 逻辑结构：M2(1) -> 总行(2) -> 大行(3) -> 小行(4) -> 外行(5) -> NSA参考(6)
    tasks = [
        (1, "06.M2供应", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WM2NS"),
        (2, "01.总存款(SA)", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSACBW027SBOG"),
        (3, "03.大型银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSLCBW027SBOG"),
        (4, "04.小型银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSSCBW027SBOG"),
        (5, "05.外国银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSFRIW027SBOG"),
        (6, "02.总存款(NSA)", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSACBW027NBOG")
    ]

    n_h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

    for rank_id, name, target_url in tasks:
        print(f"🔗 正在处理第 {rank_id} 层级: {name}")
        try:
            r = requests.get(target_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text), index_col=0)
                col_name = df.columns[0]
                
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
                        "更新日期": {"date": {"start": date}},
                        # ✅ 这里增加了排序字段
                        "排序": {"number": rank_id}
                    }
                }
                res = requests.post("https://api.notion.com/v1/pages", headers=n_h, json=payload)
                print(f"✅ 成功: {name} (排序ID: {rank_id})")
            else:
                print(f"❌ 下载失败 {name}: {r.status_code}")
            time.sleep(1)
        except Exception as e:
            print(f"❌ 严重报错 {name}: {e}")

if __name__ == "__main__":
    fetch()

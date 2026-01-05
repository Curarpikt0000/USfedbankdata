import os, requests, pandas as pd, io, time

def fetch():
    print("🚀 DEBUG: 切换到官方 Graph 接口 V2026-Final...")
    
    tok = os.getenv("NOTION_TOKEN")
    dbid = os.getenv("NOTION_DATABASE_ID")
    
    # 🔴 关键修改：使用了更稳定的 graph/fredgraph.csv 接口
    tasks = [
        ("01.总存款(SA)", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPSACBW027SBOG"),
        ("02.总存款(NSA)", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DPNSBW027SBOG"),
        ("03.大型银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DLCBW027SBOG"),
        ("04.小型银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DSCBW027SBOG"),
        ("05.外国银行", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DFXBW027SBOG"),
        ("06.M2供应", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WM2NS")
    ]

    n_h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

    for name, target_url in tasks:
        print(f"🔗 正在请求: {target_url}")
        try:
            # 增加 verify=True 确保 SSL 正常
            r = requests.get(target_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            
            if r.status_code == 200:
                # 这里的逻辑稍微调整，因为 graph 接口返回的时间序列索引在第一列
                df = pd.read_csv(io.StringIO(r.text), index_col=0)
                
                # 获取最后一行数据 (graph 接口返回的列名通常就是 ID)
                # 我们直接取第一列数据即可
                series_col = df.columns[0] 
                val = float(df.iloc[-1][series_col])
                prev = float(df.iloc[-2][series_col])
                
                change = val - prev
                date = str(df.index[-1]).split(" ")[0] # 提取日期部分
                
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
                print(f"❌ 下载失败 {name}: {r.status_code} - {r.url}")
            time.sleep(1)
        except Exception as e:
            print(f"❌ 报错 {name}: {e}")

if __name__ == "__main__":
    fetch()

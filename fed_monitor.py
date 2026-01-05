import os, requests, pandas as pd, io, time
from datetime import datetime

def fetch_history():
    print("🚀 DEBUG: 正在启动 V2026-History (3个月历史回溯版)...")
    
    tok = os.getenv("NOTION_TOKEN")
    dbid = os.getenv("NOTION_DATABASE_ID")
    
    # ✅ 核心配置：(排序, 指标名, FRED_ID, Detail指标, 类型)
    # 这里严格对应你截图 image_aaa582.jpg 的分类逻辑
    tasks = [
        (1, "06.M2供应", "WM2NS", "M2", "NONE"),
        (2, "01.总存款(SA)", "DPSACBW027SBOG", "银行整体", "SA"),
        (3, "03.大型银行", "DPSLCBW027SBOG", "大型银行", "SA"),
        (4, "04.小型银行", "DPSSCBW027SBOG", "小型银行", "SA"),
        (5, "05.外国银行", "DPSFRIW027SBOG", "外国银行", "SA"),
        (6, "02.总存款(NSA)", "DPSACBW027NBOG", "银行整体", "NSA")
    ]

    n_h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json", "Notion-Version": "2022-06-28"}

    for rank, name, series_id, detail_tag, type_tag in tasks:
        print(f"\n📊 正在处理: {name}...")
        
        # 使用 graph 接口下载 CSV
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code == 200:
                df = pd.read_csv(io.StringIO(r.text), index_col=0, parse_dates=True)
                
                # 🛑 核心逻辑：取最后 14 行 (约3个月)
                # 我们需要14周的数据来计算13周的“变化量”
                recent_df = df.tail(14)
                
                if len(recent_df) < 2:
                    print("   ⚠️ 数据不足，跳过")
                    continue

                col_name = recent_df.columns[0]
                
                # 循环处理每一周，生成历史数据
                # 从第1行开始遍历（因为第0行没有前一周，无法算变化量）
                for i in range(1, len(recent_df)):
                    curr_date = recent_df.index[i]
                    val = float(recent_df.iloc[i][col_name])
                    prev = float(recent_df.iloc[i-1][col_name])
                    change = val - prev
                    date_str = curr_date.strftime('%Y-%m-%d')
                    
                    payload = {
                        "parent": {"database_id": dbid},
                        "properties": {
                            "指标名称": {"title": [{"text": {"content": name}}]},
                            "本周余额(十亿)": {"number": round(val, 1)},
                            "周变化量": {"number": round(change, 1)},
                            "趋势": {"rich_text": [{"text": {"content": "🟢增加" if change > 0 else "🔴减少"}}]},
                            "更新日期": {"date": {"start": date_str}},
                            "排序": {"number": rank},
                            
                            # ✅ 对应 Notion 的 Select 列
                            "Detail指标": {"select": {"name": detail_tag}},
                            "类型": {"select": {"name": type_tag}}
                        }
                    }
                    
                    # 推送一条数据
                    res = requests.post("https://api.notion.com/v1/pages", headers=n_h, json=payload)
                    
                    if res.status_code == 200:
                        print(f"   ✅ {date_str} 入库成功")
                    else:
                        print(f"   ❌ {date_str} 失败: {res.text}")
                        
                    # 稍微休息一下，避免触发 Notion API 频率限制
                    time.sleep(0.2)
            else:
                print(f"❌ FRED 下载失败: {r.status_code}")
        except Exception as e:
            print(f"❌ 严重报错: {e}")

if __name__ == "__main__":
    fetch_history()

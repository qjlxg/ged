import os
import glob
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 策略参数
RSI_LIMIT = 30
BIAS_LIMIT = -4.0
PREMIUM_LIMIT = 1.0 

def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def process_file(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        # 兼容你的CSV列名：收盘, premium_rate
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        
        latest = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        if (latest['rsi'] < RSI_LIMIT and 
            latest['bias'] < BIAS_LIMIT and 
            latest.get('premium_rate', 0) < PREMIUM_LIMIT):
            return {
                '日期': latest['日期'],
                '代码': code,
                '价格': round(latest['收盘'], 4),
                'RSI': round(latest['rsi'], 2),
                'BIAS': round(latest['bias'], 2),
                '溢价率': round(latest.get('premium_rate', 0), 2)
            }
    except: return None

def update_readme(current_signals, perf_df):
    """将结果写入 README.md"""
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    content = f"# ETF 策略监控自动化\n\n"
    content += f"**最后更新时间 (北京时间):** `{now_str}`\n\n"
    
    content += "## 🎯 当前实时筛选信号 (RSI<30 & BIAS<-4%)\n"
    if current_signals:
        sig_df = pd.DataFrame(current_signals)
        content += sig_df.to_markdown(index=False) + "\n\n"
    else:
        content += "*当前暂无满足条件的信号，继续空仓等待。*\n\n"
    
    content += "## 📈 历史复盘战绩 (次日表现)\n"
    if not perf_df.empty:
        # 只展示最近10条记录
        recent_perf = perf_df.tail(10).iloc[::-1] 
        win_rate = (perf_df['结果'] == '涨').sum() / len(perf_df) * 100
        content += f"**累计总信号数:** `{len(perf_df)}` | **次日上涨概率:** `{win_rate:.2f}%` \n\n"
        content += recent_perf.to_markdown(index=False) + "\n\n"
    else:
        content += "*暂无历史复盘数据。*\n"
    
    with open('README.md', 'w', encoding='utf-8') as f:
        f.write(content)

def get_performance():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'track' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code'])
                raw_path = f'fund_data/{code}.csv'
                if os.path.exists(raw_path):
                    raw_df = pd.read_csv(raw_path)
                    idx = raw_df[raw_df['日期'] == sig['date']].index
                    if len(idx) > 0 and (idx[0] + 1) < len(raw_df):
                        next_day = raw_df.iloc[idx[0] + 1]
                        change = (next_day['收盘'] - sig['price']) / sig['price'] * 100
                        perf_list.append({
                            '信号日期': sig['date'], '代码': code, '买入价': sig['price'],
                            '次日收盘': next_day['收盘'], '次日涨跌%': round(change, 2),
                            '结果': '涨' if change > 0 else '跌'
                        })
        except: continue
    return pd.DataFrame(perf_list)

def main():
    data_dir = 'fund_data'
    if not os.path.exists(data_dir): return
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    
    # 存一份 CSV 存档用于复盘计算
    if results:
        now = datetime.now()
        out_path = now.strftime('%Y/%m')
        os.makedirs(out_path, exist_ok=True)
        # 存档用的字段名保持与复盘逻辑一致
        archive_df = pd.DataFrame(results).rename(columns={'日期':'date', '代码':'fund_code', '价格':'price'})
        archive_df.to_csv(os.path.join(out_path, f"signals_{now.strftime('%H%M%S')}.csv"), index=False)
    
    perf_df = get_performance()
    # 更新 README.md 页面
    update_readme(results, perf_df)
    
    # 同时保留 performance_reports
    if not perf_df.empty:
        os.makedirs('performance_reports', exist_ok=True)
        perf_df.to_csv('performance_reports/history_track.csv', index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    main()

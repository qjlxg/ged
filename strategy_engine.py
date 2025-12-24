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
    """纯 pandas 实现 RSI 计算，不依赖外部库"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def process_file(file_path):
    """扫描 fund_data 目录"""
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 计算 RSI(6) 和 BIAS(6)
        df['rsi'] = calculate_rsi(df['close'], 6)
        df['ma6'] = df['close'].rolling(window=6).mean()
        df['bias'] = ((df['close'] - df['ma6']) / df['ma6']) * 100
        
        latest = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 满足：RSI超卖 + BIAS负乖离 + 低溢价
        if (latest['rsi'] < RSI_LIMIT and 
            latest['bias'] < BIAS_LIMIT and 
            latest.get('premium_rate', 0) < PREMIUM_LIMIT):
            return {
                'date': latest['date'],
                'fund_code': code,
                'price': round(latest['close'], 4),
                'rsi': round(latest['rsi'], 2),
                'bias': round(latest['bias'], 2),
                'premium_rate': round(latest.get('premium_rate', 0), 2)
            }
    except: return None

def update_performance():
    """复盘：追踪历史信号次日表现"""
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'performance' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code'])
                raw_path = f'fund_data/{code}.csv'
                if os.path.exists(raw_path):
                    raw_df = pd.read_csv(raw_path)
                    idx = raw_df[raw_df['date'] == sig['date']].index
                    if len(idx) > 0 and (idx[0] + 1) < len(raw_df):
                        next_day = raw_df.iloc[idx[0] + 1]
                        change = (next_day['close'] - sig['price']) / sig['price'] * 100
                        perf_list.append({
                            '信号日期': sig['date'], '代码': code, '买入价': sig['price'],
                            '次日收盘': next_day['close'], '次日涨跌%': round(change, 2),
                            '结果': '涨' if change > 0 else '跌'
                        })
        except: continue
    if perf_list:
        os.makedirs('performance_reports', exist_ok=True)
        pd.DataFrame(perf_list).to_csv('performance_reports/history_track.csv', index=False, encoding='utf-8-sig')

def main():
    data_dir = 'fund_data'
    if not os.path.exists(data_dir): return
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    if results:
        now = datetime.now()
        out_path = now.strftime('%Y/%m')
        os.makedirs(out_path, exist_ok=True)
        pd.DataFrame(results).to_csv(os.path.join(out_path, f"signals_{now.strftime('%H%M%S')}.csv"), index=False)
    update_performance()

if __name__ == "__main__":
    main()

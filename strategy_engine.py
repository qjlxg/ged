import os
import glob
import pandas as pd
import pandas_ta as ta
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 策略参数：RSI < 30 + BIAS < -4% + 溢价 < 1%
RSI_LIMIT = 30
BIAS_LIMIT = -4.0
PREMIUM_LIMIT = 1.0 

def process_file(file_path):
    """并行扫描：从 fund_data 目录提取信号"""
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 计算指标
        df['rsi'] = ta.rsi(df['close'], length=6)
        df['ma6'] = df['close'].rolling(window=6).mean()
        df['bias'] = ((df['close'] - df['ma6']) / df['ma6']) * 100
        
        latest = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 触发筛选
        if (latest['rsi'] < RSI_LIMIT and 
            latest['bias'] < BIAS_LIMIT and 
            latest['premium_rate'] < PREMIUM_LIMIT):
            return {
                'date': latest['date'],
                'fund_code': code,
                'price': round(latest['close'], 4),
                'rsi': round(latest['rsi'], 2),
                'bias': round(latest['bias'], 2),
                'premium_rate': round(latest['premium_rate'], 2)
            }
    except: return None

def update_performance():
    """复盘功能：扫描历史记录并对比次日涨跌"""
    # 搜索仓库内所有年月目录下的信号文件
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
                    # 定位信号日并获取后一天
                    idx = raw_df[raw_df['date'] == sig['date']].index
                    if len(idx) > 0 and (idx[0] + 1) < len(raw_df):
                        next_day = raw_df.iloc[idx[0] + 1]
                        change = (next_day['close'] - sig['price']) / sig['price'] * 100
                        perf_list.append({
                            '信号日期': sig['date'],
                            '代码': code,
                            '买入价': sig['price'],
                            '次日收盘': next_day['close'],
                            '次日涨跌%': round(change, 2),
                            '结果': '涨' if change > 0 else '跌'
                        })
        except: continue
        
    if perf_list:
        os.makedirs('performance_reports', exist_ok=True)
        pd.DataFrame(perf_list).to_csv('performance_reports/history_track.csv', index=False, encoding='utf-8-sig')

def main():
    # 1. 扫描新信号
    data_dir = 'fund_data'
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    
    # 2. 存档新信号
    if results:
        now = datetime.now()
        out_path = now.strftime('%Y/%m')
        os.makedirs(out_path, exist_ok=True)
        file_name = f"signals_{now.strftime('%H%M%S')}.csv"
        pd.DataFrame(results).to_csv(os.path.join(out_path, file_name), index=False)

    # 3. 统计全历史表现
    update_performance()

if __name__ == "__main__":
    main()

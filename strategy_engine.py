import os
import glob
import pandas as pd
import pandas_ta as ta
from datetime import datetime
from multiprocessing import Pool, cpu_count

# 核心筛选参数
RSI_PERIOD = 6
BIAS_PERIOD = 6
RSI_LIMIT = 30
BIAS_LIMIT = -4.0
PREMIUM_LIMIT = 1.0  # 对应 CSV 中的 premium_rate 列

def process_file(file_path):
    """并行扫描：计算指标并筛选信号"""
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 计算 RSI(6)
        df['rsi'] = ta.rsi(df['close'], length=RSI_PERIOD)
        # 计算 BIAS(6)
        df['ma'] = df['close'].rolling(window=BIAS_PERIOD).mean()
        df['bias'] = ((df['close'] - df['ma']) / df['ma']) * 100
        
        latest = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0]
        
        # 筛选逻辑：超跌(RSI) + 空间(BIAS) + 价格水分(Premium)
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

def update_performance_report():
    """复盘：追踪历史信号次日的涨跌表现"""
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_data = []
    
    for h_file in history_files:
        if 'report' in h_file: continue
        signals = pd.read_csv(h_file)
        for _, sig in signals.iterrows():
            code = str(sig['fund_code'])
            raw_path = f'fund_data/{code}.csv'
            if os.path.exists(raw_path):
                raw_df = pd.read_csv(raw_path)
                # 寻找信号日期的次日数据
                idx = raw_df[raw_df['date'] == sig['date']].index
                if len(idx) > 0 and (idx[0] + 1) < len(raw_df):
                    next_day = raw_df.iloc[idx[0] + 1]
                    pct_change = (next_day['close'] - sig['price']) / sig['price'] * 100
                    perf_data.append({
                        '信号日期': sig['date'],
                        '代码': code,
                        '买入价': sig['price'],
                        '次日收盘': next_day['close'],
                        '次日涨跌%': round(pct_change, 2),
                        '结果': '✅胜' if pct_change > 0 else '❌负'
                    })
    
    if perf_data:
        report_df = pd.DataFrame(perf_data)
        os.makedirs('performance_reports', exist_ok=True)
        report_df.to_csv('performance_reports/history_track.csv', index=False, encoding='utf-8-sig')

def main():
    # 1. 运行当前扫描
    data_dir = 'fund_data'
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.csv')]
    with Pool(cpu_count()) as p:
        current_results = [r for r in p.map(process_file, files) if r is not None]
    
    # 2. 保存新信号 (按年月归档)
    if current_results:
        now = datetime.now()
        save_path = now.strftime('%Y/%m')
        os.makedirs(save_path, exist_ok=True)
        file_name = f"signals_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        pd.DataFrame(current_results).to_csv(os.path.join(save_path, file_name), index=False)

    # 3. 自动复盘历史记录
    update_performance_report()

if __name__ == "__main__":
    main()

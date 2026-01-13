import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 脚本说明：Alpha Hunter V6 精简决策版
# 逻辑依据：RSI(超买超卖) + BIAS(均线偏离) + VOLUME(量价验证) + ATR(波动适配)
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 
TRACKER_FILE = 'signal_tracker.csv' 

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def analyze_fund(file_path):
    try:
        full_df = pd.read_csv(file_path, encoding='utf-8-sig')
        if len(full_df) < 60: return None
        full_df.columns = [c.strip() for c in full_df.columns]
        df = full_df.tail(120).copy()
        latest = df.iloc[-1]
        
        # 核心指标计算
        ma20 = df['收盘'].rolling(20).mean().iloc[-1]
        rsi_val = calculate_rsi(df['收盘']).iloc[-1]
        vol_ratio = df['成交额'].tail(5).mean() / (df['成交额'].tail(20).mean() + 1e-9)
        bias = (latest['收盘'] - ma20) / ma20 * 100
        
        # 判定结论
        signal_type = "观望"
        reason = ""
        
        # 买入逻辑：情绪低位 + 价格跌透
        if rsi_val < 38:
            signal_type = "买入"
            reason = "超跌捡漏"
            if rsi_val < 32 and bias < -4:
                reason = "极度超跌(黄金底)"
        
        # 卖出逻辑：情绪高位 或 缩量拉升风险
        elif rsi_val > 70:
            signal_type = "卖出"
            reason = "涨幅过大(风险高)"
        elif latest['收盘'] > ma20 and vol_ratio < 0.8:
            signal_type = "卖出"
            reason = "缩量上涨(诱多风险)"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            'code': code, 'price': latest['收盘'], 'rsi': rsi_val, 
            'signal': signal_type, 'reason': reason, 'is_signal': (signal_type == "买入"),
            'date': latest['日期'] if '日期' in latest else datetime.now().strftime('%Y-%m-%d')
        }
    except: return None

def main():
    # 加载名称
    target_file = ETF_LIST_FILE if os.path.exists(ETF_LIST_FILE) else ETF_LIST_FILE.replace('.xlsx', '.csv')
    try:
        if target_file.endswith('.xlsx'): name_df = pd.read_excel(target_file)
        else: name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df['证券代码'] = name_df['证券代码'].astype(str).str.zfill(6)
        name_map = dict(zip(name_df['证券代码'], name_df['证券简称']))
    except: return

    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(analyze_fund, csv_files) if r and r['code'] in name_map]
    
    if not results: return
    
    # 分类
    buy_list = [r for r in results if r['signal'] == "买入"]
    sell_list = [r for r in results if r['signal'] == "卖出"]
    
    # 打印结果
    print(f"\n📅 分析日期: {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)
    
    print("🟢 【买入清单】 - 建议分批建仓或加仓：")
    if buy_list:
        for item in sorted(buy_list, key=lambda x: x['rsi']):
            print(f"代码: {item['code']} | 简称: {name_map[item['code']]:<10} | 现价: {item['price']:<7} | 理由: {item['reason']}")
    else:
        print("   (当前市场较热，暂无推荐买入品种)")
        
    print("-" * 60)
    
    print("🔴 【卖出清单】 - 建议减仓或止盈避险：")
    if sell_list:
        for item in sorted(sell_list, key=lambda x: x['rsi'], reverse=True):
            print(f"代码: {item['code']} | 简称: {name_map[item['code']]:<10} | 现价: {item['price']:<7} | 理由: {item['reason']}")
    else:
        print("   (暂无急需卖出品种)")
    print("=" * 60)
    print("💡 提示：不在清单中的品种建议维持现有网格正常运行。")

if __name__ == "__main__":
    main()

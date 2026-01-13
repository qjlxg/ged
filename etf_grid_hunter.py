import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 脚本说明：Alpha Hunter V6 官方决策版 (完整功能/精简输出)
# 依据：利用 RSI(情绪)、BIAS(乖离)、VOLUME(量价) 及 ATR(波动) 综合判定
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 
TRACKER_FILE = 'signal_tracker.csv' # 胜率账本：始终存在，自动更新

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
        
        # --- 核心依据逻辑 ---
        # 1. 情绪指标 (RSI)
        rsi_val = calculate_rsi(df['收盘']).iloc[-1]
        # 2. 均线偏离 (BIAS)
        ma20 = df['收盘'].rolling(20).mean().iloc[-1]
        bias = (latest['收盘'] - ma20) / ma20 * 100
        # 3. 成交量验证 (量比)
        vol_ratio = df['成交额'].tail(5).mean() / (df['成交额'].tail(20).mean() + 1e-9)
        
        # --- 决策系统 ---
        signal_type = "持仓观望"
        reason = ""
        is_buy_signal = False

        if rsi_val < 38: # 依据：超跌
            signal_type = "建议买入"
            reason = "情绪冰点/低位建仓"
            is_buy_signal = True
            if rsi_val < 32: reason = "严重超跌/黄金底"
        elif rsi_val > 70: # 依据：超买
            signal_type = "建议卖出"
            reason = "情绪过热/逢高止盈"
        elif latest['收盘'] > ma20 and vol_ratio < 0.8: # 依据：量价背离
            signal_type = "建议卖出"
            reason = "缩量上涨/诱多风险"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            'analysis': {
                '代码': code, '现价': latest['收盘'], 'rsi': rsi_val, 
                '信号': signal_type, '理由': reason, 'is_signal': is_buy_signal,
                'date': latest['日期'] if '日期' in latest else datetime.now().strftime('%Y-%m-%d')
            },
            'history': full_df[['日期', '收盘']]
        }
    except: return None

def update_tracker(new_results, hist_map):
    """维护历史胜率账本"""
    cols = ['代码', '入场日期', '买入价', 'T+7收益%', 'T+14收益%', 'T+20收益%', 'T+60收益%', '状态']
    if os.path.exists(TRACKER_FILE):
        tracker = pd.read_csv(TRACKER_FILE)
    else:
        tracker = pd.DataFrame(columns=cols)

    # 记录新买入信号
    for item in new_results:
        if item['is_signal']:
            recent = tracker[tracker['代码'] == item['代码']].tail(1)
            if recent.empty or (datetime.now() - pd.to_datetime(recent['入场日期'].values[0])).days > 10:
                new_row = pd.DataFrame([[item['代码'], item['date'], item['现价'], np.nan, np.nan, np.nan, np.nan, '持有中']], columns=cols)
                tracker = pd.concat([tracker, new_row], ignore_index=True)

    # 刷新历史表现
    for idx, row in tracker.iterrows():
        code = str(row['代码']).zfill(6)
        if code in hist_map:
            h_df = hist_map[code].copy()
            h_df['日期'] = pd.to_datetime(h_df['日期'])
            buy_dt = pd.to_datetime(row['入场日期'])
            future = h_df[h_df['日期'] > buy_dt]
            if not future.empty:
                for t in [7, 14, 20, 60]:
                    col = f'T+{t}收益%'
                    if pd.isna(row[col]) and len(future) >= t:
                        p_t = future.iloc[t-1]['收盘']
                        tracker.at[idx, col] = round((p_t - row['买入价']) / row['买入价'] * 100, 2)
                if len(future) >= 60: tracker.at[idx, '状态'] = '已结项'
    
    tracker.to_csv(TRACKER_FILE, index=False, encoding='utf-8-sig')
    return tracker

def main():
    # 1. 加载文件映射
    target_file = ETF_LIST_FILE if os.path.exists(ETF_LIST_FILE) else ETF_LIST_FILE.replace('.xlsx', '.csv')
    try:
        if target_file.endswith('.xlsx'): name_df = pd.read_excel(target_file)
        else: name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df.columns = [c.strip() for c in name_df.columns]
        name_map = dict(zip(name_df['证券代码'].astype(str).str.zfill(6), name_df['证券简称']))
    except: return

    # 2. 并行分析
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🚀 Alpha Hunter 启动：正在深度诊断 {len(csv_files)} 个品种...")
    with Pool(cpu_count()) as p:
        raw = p.map(analyze_fund, csv_files)
    
    results = [r['analysis'] for r in raw if r and r['analysis']['代码'] in name_map]
    hist_map = {r['analysis']['代码']: r['history'] for r in raw if r}

    # 3. 更新胜率账本
    tracker_df = update_tracker(results, hist_map)

    # 4. 打印官方决策清单
    buy_list = [r for r in results if r['信号'] == "建议买入"]
    sell_list = [r for r in results if r['信号'] == "建议卖出"]

    print(f"\n📅 诊断报告日期: {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 70)
    print("🟢 【买入执行清单】 (当前被低估，具备反弹潜力)")
    if buy_list:
        for r in sorted(buy_list, key=lambda x: x['rsi']):
            print(f"  代码: {r['代码']} | 简称: {name_map[r['代码']]:<10} | 现价: {r['现价']:<8} | 理由: {r['理由']}")
    else:
        print("  (当前市场较热，无建议买入品种)")

    print("-" * 70)
    print("🔴 【卖出执行清单】 (当前过热或缩量，风险较大)")
    if sell_list:
        for r in sorted(sell_list, key=lambda x: x['rsi'], reverse=True):
            print(f"  代码: {r['代码']} | 简称: {name_map[r['代码']]:<10} | 现价: {r['现价']:<8} | 理由: {r['理由']}")
    else:
        print("  (暂无建议卖出品种)")
    print("=" * 70)

    # 5. 打印胜率简报
    print("\n📊 策略可信度验证 (基于 signal_tracker.csv 历史记录):")
    for t in [7, 14, 20, 60]:
        col = f'T+{t}收益%'
        valid = tracker_df[tracker_df[col].notna()]
        if not valid.empty:
            wr = (valid[col].astype(float) > 0).sum() / len(valid) * 100
            avg = valid[col].astype(float).mean()
            print(f" >> T+{t} 历史胜率: {wr:.1f}% | 平均收益: {avg:.2f}% (样本数: {len(valid)})")
    print("-" * 70)

if __name__ == "__main__":
    main()

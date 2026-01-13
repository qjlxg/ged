import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：Alpha Hunter V7 官方决策全功能版
# 1. [判定依据]：RSI(超买超卖)、BIAS(均线偏离)、VOLUME(量价验证)、ATR(波动适配)
# 2. [历史记账]：自动维护 signal_tracker.csv，计算 T+7/14/20/60 真实胜率
# 3. [决策存档]：每日建议自动追加到 final_decision_log.csv，方便查阅历史
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 
TRACKER_FILE = 'signal_tracker.csv'    # 胜率回测账本
DECISION_LOG = 'final_decision_log.csv' # 每日买卖决策记录

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
        
        # --- [硬核指标计算] ---
        # 1. 情绪温度 (RSI)
        rsi_val = calculate_rsi(df['收盘']).iloc[-1]
        # 2. 均线偏离度 (BIAS)
        ma20 = df['收盘'].rolling(20).mean().iloc[-1]
        bias = (latest['收盘'] - ma20) / ma20 * 100
        # 3. 人气量能 (VOLUME)
        vol_ratio = df['成交额'].tail(5).mean() / (df['成交额'].tail(20).mean() + 1e-9)
        # 4. 波动性格 (ATR)
        atr = (df['最高'] - df['最低']).rolling(14).mean().iloc[-1]
        volatility = (atr / latest['收盘']) * 100

        # --- [决策逻辑系统] ---
        signal_type, reason, is_buy = "观望", "正常波动", False

        # 买入依据：极度冷清 + 价格跌破位
        if rsi_val < 68:#rsi_val < 38
            signal_type, reason, is_buy = "建议买入", "情绪冰点/低位建仓", True
            if rsi_val < 32: reason = "严重超跌/黄金底"
        # 卖出依据：情绪过热 或 缩量拉升（诱多）
        elif rsi_val > 70:
            signal_type, reason = "建议卖出", "情绪过热/逢高止盈"
        elif latest['收盘'] > ma20 and vol_ratio < 0.8:
            signal_type, reason = "建议卖出", "量价背离/警惕诱多"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            'analysis': {
                '日期': latest['日期'] if '日期' in latest else datetime.now().strftime('%Y-%m-%d'),
                '代码': code, '价格': latest['收盘'], 'RSI': round(rsi_val, 1), 
                '信号': signal_type, '理由': reason, 'is_signal': is_buy,
                '偏离度%': round(bias, 2), '人气值': round(vol_ratio, 2)
            },
            'history': full_df[['日期', '收盘']]
        }
    except: return None

def update_tracker(new_results, hist_map):
    """历史胜率回测账本维护"""
    cols = ['代码', '入场日期', '买入价', 'T+7收益%', 'T+14收益%', 'T+20收益%', 'T+60收益%', '状态']
    tracker = pd.read_csv(TRACKER_FILE) if os.path.exists(TRACKER_FILE) else pd.DataFrame(columns=cols)
    
    # 记录新买入信号
    for item in new_results:
        if item['is_signal']:
            recent = tracker[tracker['代码'] == item['代码']].tail(1)
            if recent.empty or (datetime.now() - pd.to_datetime(recent['入场日期'].values[0])).days > 10:
                new_row = pd.DataFrame([[item['代码'], item['日期'], item['价格'], np.nan, np.nan, np.nan, np.nan, '持有中']], columns=cols)
                tracker = pd.concat([tracker, new_row], ignore_index=True)

    # 刷新已记录信号的收益
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
    # 1. 资源准备
    target_file = ETF_LIST_FILE if os.path.exists(ETF_LIST_FILE) else ETF_LIST_FILE.replace('.xlsx', '.csv')
    try:
        if target_file.endswith('.xlsx'): name_df = pd.read_excel(target_file)
        else: name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df.columns = [c.strip() for c in name_df.columns]
        name_map = dict(zip(name_df['证券代码'].astype(str).str.zfill(6), name_df['证券简称']))
    except: return

    # 2. 并行扫描
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🚀 Alpha Hunter 启动：深度诊断 {len(csv_files)} 个品种...")
    with Pool(cpu_count()) as p:
        raw_output = p.map(analyze_fund, csv_files)
    
    results = [r['analysis'] for r in raw_output if r and r['analysis']['代码'] in name_map]
    hist_map = {r['analysis']['代码']: r['history'] for r in raw_output if r}

    # 3. 记账与持久化
    tracker_df = update_tracker(results, hist_map)
    
    # 整理决策记录
    decisions = [r for r in results if r['信号'] in ["建议买入", "建议卖出"]]
    if decisions:
        log_df = pd.DataFrame(decisions)
        log_df['简称'] = log_df['代码'].apply(lambda x: name_map.get(x, '未知'))
        # 调整列顺序保存
        save_cols = ['日期', '代码', '简称', '价格', '信号', '理由', 'RSI', '偏离度%', '人气值']
        log_df = log_df[save_cols]
        header = not os.path.exists(DECISION_LOG)
        log_df.to_csv(DECISION_LOG, mode='a', index=False, header=header, encoding='utf-8-sig')

    # 4. 控制台精简输出
    print(f"\n📅 诊断日期: {datetime.now().strftime('%Y-%m-%d')}")
    print(f"💾 决策记录已更新至: {DECISION_LOG}")
    print("=" * 80)
    
    buy_list = [r for r in results if r['信号'] == "建议买入"]
    sell_list = [r for r in results if r['信号'] == "建议卖出"]

    print("🟢 【建议买入清单】")
    if buy_list:
        for r in sorted(buy_list, key=lambda x: x['RSI']):
            print(f"  {r['代码']} | {name_map[r['代码']]:<12} | 现价:{r['价格']:<7} | {r['理由']}")
    else: print("  (市场火热，暂无低吸机会)")

    print("-" * 80)
    print("🔴 【建议卖出清单】")
    if sell_list:
        for r in sorted(sell_list, key=lambda x: x['RSI'], reverse=True):
            print(f"  {r['代码']} | {name_map[r['代码']]:<12} | 现价:{r['价格']:<7} | {r['理由']}")
    else: print("  (暂无高风险抛售品种)")
    
    print("=" * 80)

    # 5. 打印胜率统计简报
    print("\n📊 历史信号可靠性验证 (基于历史模拟买入记录):")
    for t in [7, 14, 20, 60]:
        col = f'T+{t}收益%'
        if col in tracker_df.columns:
            valid = tracker_df[tracker_df[col].notna()]
            if not valid.empty:
                wr = (valid[col].astype(float) > 0).sum() / len(valid) * 100
                print(f" >> T+{t}天 胜率: {wr:.1f}% | 样本量: {len(valid)}")
            else: print(f" >> T+{t}天 样本收集阶段...")

if __name__ == "__main__":
    main()

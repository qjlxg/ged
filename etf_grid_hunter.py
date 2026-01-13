import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：Alpha Hunter V8.5 终极实战版
# 1. [判定维度]：RSI(情绪)、BIAS(均线引力)、VOLUME(量价真伪)、ATR(波动性格)
# 2. [存储逻辑]：每日建议存入 results/年/月/ 目录；胜率账本 signal_tracker.csv 持续累加
# 3. [排序映射]：买入在前并按 RSI 升序排列，所有输出均包含基金简称映射
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 
TRACKER_FILE = 'signal_tracker.csv'    # 历史胜率账本
BASE_RESULT_DIR = 'results'            # 归档根目录

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
        
        # --- 核心指标计算 ---
        rsi_val = calculate_rsi(df['收盘']).iloc[-1]
        ma20 = df['收盘'].rolling(20).mean().iloc[-1]
        bias = (latest['收盘'] - ma20) / ma20 * 100
        vol_ratio = df['成交额'].tail(5).mean() / (df['成交额'].tail(20).mean() + 1e-9)
        
        # --- 决策逻辑 ---
        signal_type, reason, is_buy = "观望", "正常波动", False

        if rsi_val < 43: # 买入阈值
            signal_type, reason, is_buy = "建议买入", "情绪冰点", True
            if rsi_val < 32: reason = "严重超跌/黄金坑"
        elif rsi_val > 70: # 卖出阈值
            signal_type, reason = "建议卖出", "情绪过热"
        elif latest['收盘'] > ma20 and vol_ratio < 0.8: # 量价背离
            signal_type, reason = "建议卖出", "量价背离/缩量诱多"

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

def update_tracker(new_results, hist_map, name_map):
    """维护回测账本，确保列名一致性"""
    cols = ['代码', '简称', '入场日期', '买入价', 'T+7收益%', 'T+14收益%', 'T+20收益%', 'T+60收益%', '状态']
    if os.path.exists(TRACKER_FILE):
        tracker = pd.read_csv(TRACKER_FILE)
        # 补齐可能缺失的“简称”列
        if '简称' not in tracker.columns:
            tracker.insert(1, '简称', tracker['代码'].apply(lambda x: name_map.get(str(x).zfill(6), '')))
    else:
        tracker = pd.DataFrame(columns=cols)

    # 1. 记录今日新买入信号
    for item in new_results:
        if item['is_signal']:
            recent = tracker[tracker['代码'] == item['代码']].tail(1)
            # 冷却期10天，避免重复记账
            if recent.empty or (datetime.now() - pd.to_datetime(recent['入场日期'].values[0])).days > 10:
                new_row = pd.DataFrame([[
                    item['代码'], name_map.get(item['代码'], '未知'), item['日期'], 
                    item['价格'], np.nan, np.nan, np.nan, np.nan, '持有中'
                ]], columns=cols)
                tracker = pd.concat([tracker, new_row], ignore_index=True)

    # 2. 刷新收益情况
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
    # --- 加载基金列表与名称映射 ---
    target_file = ETF_LIST_FILE if os.path.exists(ETF_LIST_FILE) else ETF_LIST_FILE.replace('.xlsx', '.csv')
    try:
        if target_file.endswith('.xlsx'): name_df = pd.read_excel(target_file)
        else: name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df.columns = [c.strip() for c in name_df.columns]
        name_map = dict(zip(name_df['证券代码'].astype(str).str.zfill(6), name_df['证券简称']))
    except:
        print("❌ 错误：无法读取 ETF 列表文件，请确认路径。")
        return

    # --- 并行扫描分析 ---
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🚀 Alpha Hunter V8.5 启动：正在深度诊断 {len(csv_files)} 个品种...")
    with Pool(cpu_count()) as p:
        raw_output = p.map(analyze_fund, csv_files)
    
    results = [r['analysis'] for r in raw_output if r and r['analysis']['代码'] in name_map]
    hist_map = {r['analysis']['代码']: r['history'] for r in raw_output if r}

    # --- 更新胜率回测账本 ---
    tracker_df = update_tracker(results, hist_map, name_map)

    # --- 处理今日决策与排序 ---
    buy_list = sorted([r for r in results if r['信号'] == "建议买入"], key=lambda x: x['RSI'])
    sell_list = sorted([r for r in results if r['信号'] == "建议卖出"], key=lambda x: x['RSI'], reverse=True)
    all_decisions = buy_list + sell_list

    # --- 生成年月归档文件 ---
    now = datetime.now()
    dir_path = os.path.join(BASE_RESULT_DIR, now.strftime('%Y'), now.strftime('%m'))
    os.makedirs(dir_path, exist_ok=True)
    full_path = os.path.join(dir_path, f"scan_{now.strftime('%Y%m%d')}.csv")

    if all_decisions:
        output_df = pd.DataFrame(all_decisions)
        # 插入简称到第二列
        output_df.insert(2, '简称', output_df['代码'].apply(lambda x: name_map.get(x, '未知')))
        output_df.to_csv(full_path, index=False, encoding='utf-8-sig')
        print(f"💾 今日决策已归档至: {full_path}")

    # --- 控制台可视化显示 ---
    print(f"\n📅 分析报告日期: {now.strftime('%Y-%m-%d')}")
    print("=" * 100)
    print(f"{'代码':<8} | {'简称':<12} | {'价格':<8} | {'RSI':<5} | {'偏离度%':<8} | {'理由'}")
    print("-" * 100)
    
    for r in buy_list:
        print(f"🟢 {r['代码']:<6} | {name_map[r['代码']]:<10} | {r['价格']:<8} | {r['RSI']:<5} | {r['偏离度%']:<8} | {r['信号']}({r['理由']})")
    for r in sell_list:
        print(f"🔴 {r['代码']:<6} | {name_map[r['代码']]:<10} | {r['价格']:<8} | {r['RSI']:<5} | {r['偏离度%']:<8} | {r['信号']}({r['理由']})")
    
    if not all_decisions:
        print("   (当前暂无极端信号，行情处于平稳区间，网格策略按兵不动)")
    print("=" * 100)

    # --- 胜率复盘简报 ---
    print("\n📊 历史信号可靠性报告 (Based on signal_tracker.csv):")
    for t in [7, 14, 20, 60]:
        col = f'T+{t}收益%'
        if col in tracker_df.columns:
            valid = tracker_df[tracker_df[col].notna()]
            if not valid.empty:
                wr = (valid[col].astype(float) > 0).sum() / len(valid) * 100
                avg = valid[col].astype(float).mean()
                print(f" >> T+{t:2d}表现: 胜率 {wr:5.1f}% | 平均收益 {avg:5.2f}% (样本数:{len(valid)})")

if __name__ == "__main__":
    main()

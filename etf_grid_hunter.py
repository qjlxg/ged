import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：Alpha Hunter V8.1 终极实战归档版
# 核心指标依据：
# 1. RSI (14日情绪温度)：判定市场是否跌透(超卖)或涨疯(超买)
# 2. BIAS (乖离率)：测量价格偏离20日均线的距离，利用均线引力捕捉反弹
# 3. VOLUME (量价验证)：通过5日均量/20日均量比值，识别“缩量上涨”的骗炮风险
# 4. ATR (真实波幅)：动态适配品种性格，计算更精准的横盘与波动区间
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 
TRACKER_FILE = 'signal_tracker.csv'    # 历史信号回测账本（根目录）
BASE_RESULT_DIR = 'results'            # 决策报告存放根目录

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
        
        # 获取分析所需的计算片段
        df = full_df.tail(120).copy()
        latest = df.iloc[-1]
        close_s = df['收盘']
        vol_s = df['成交额']
        
        # --- [技术指标深度计算] ---
        # 1. 情绪温度 (RSI)
        rsi_val = calculate_rsi(close_s).iloc[-1]
        
        # 2. 均线引力 (BIAS)
        ma20 = close_s.rolling(20).mean().iloc[-1]
        bias = (latest['收盘'] - ma20) / ma20 * 100
        
        # 3. 资金活跃度 (成交量比)
        vol_ratio = vol_s.tail(5).mean() / (vol_s.tail(20).mean() + 1e-9)
        
        # 4. 波动率适配 (ATR)
        high_low = df['最高'] - df['最低']
        atr = high_low.rolling(14).mean().iloc[-1]
        volatility = (atr / latest['收盘']) * 100

        # --- [多维度决策系统] ---
        signal_type, reason, is_buy = "观望", "震荡区域", False

        # 买入信号判定：必须同时满足超跌和情绪冰点
        if rsi_val < 38:
            signal_type, reason, is_buy = "建议买入", "情绪冰点/低位建仓", True
            if rsi_val < 32:
                reason = "严重超跌/黄金坑"
        
        # 卖出信号判定：超买过热 或 出现缩量诱多
        elif rsi_val > 70:
            signal_type, reason = "建议卖出", "情绪过热/逢高止盈"
        elif latest['收盘'] > ma20 and vol_ratio < 0.8:
            signal_type, reason = "建议卖出", "缩量上涨/诱多风险"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            'analysis': {
                '日期': latest['日期'] if '日期' in latest else datetime.now().strftime('%Y-%m-%d'),
                '代码': code, '价格': latest['收盘'], 'RSI': round(rsi_val, 1), 
                '信号': signal_type, '理由': reason, 'is_signal': is_buy,
                '偏离度%': round(bias, 2), '人气值': round(vol_ratio, 2),
                '波动率%': round(volatility, 2)
            },
            'history': full_df[['日期', '收盘']]
        }
    except: return None

def update_tracker(new_results, hist_map):
    """维护胜率账本，追踪买入信号后的表现"""
    cols = ['代码', '入场日期', '买入价', 'T+7收益%', 'T+14收益%', 'T+20收益%', 'T+60收益%', '状态']
    if os.path.exists(TRACKER_FILE):
        tracker = pd.read_csv(TRACKER_FILE)
    else:
        tracker = pd.DataFrame(columns=cols)

    # 1. 记录今日新产生的买入信号
    for item in new_results:
        if item['is_signal']:
            recent = tracker[tracker['代码'] == item['代码']].tail(1)
            # 10天冷却期，防止下跌过程中信号刷屏
            if recent.empty or (datetime.now() - pd.to_datetime(recent['入场日期'].values[0])).days > 10:
                new_row = pd.DataFrame([[
                    item['代码'], item['日期'], item['价格'], np.nan, np.nan, np.nan, np.nan, '持有中'
                ]], columns=cols)
                tracker = pd.concat([tracker, new_row], ignore_index=True)

    # 2. 自动刷新历史信号的后续表现
    for idx, row in tracker.iterrows():
        code = str(row['代码']).zfill(6)
        if code in hist_map:
            h_df = hist_map[code].copy()
            h_df['日期'] = pd.to_datetime(h_df['日期'])
            buy_dt = pd.to_datetime(row['入场日期'])
            future_data = h_df[h_df['日期'] > buy_dt]
            if not future_data.empty:
                for t in [7, 14, 20, 60]:
                    col = f'T+{t}收益%'
                    if pd.isna(row[col]) and len(future_data) >= t:
                        p_t = future_data.iloc[t-1]['收盘']
                        tracker.at[idx, col] = round((p_t - row['买入价']) / row['买入价'] * 100, 2)
                if len(future_data) >= 60:
                    tracker.at[idx, '状态'] = '已结项'
    
    tracker.to_csv(TRACKER_FILE, index=False, encoding='utf-8-sig')
    return tracker

def main():
    # --- 资源初始化 ---
    target_file = ETF_LIST_FILE if os.path.exists(ETF_LIST_FILE) else ETF_LIST_FILE.replace('.xlsx', '.csv')
    try:
        if target_file.endswith('.xlsx'): name_df = pd.read_excel(target_file)
        else: name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df.columns = [c.strip() for c in name_df.columns]
        name_map = dict(zip(name_df['证券代码'].astype(str).str.zfill(6), name_df['证券简称']))
    except Exception as e:
        print(f"❌ 列表文件读取失败: {e}")
        return

    # --- 并行计算分析 ---
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🚀 Alpha Hunter V8.1 启动：正在诊断 {len(csv_files)} 个品种...")
    with Pool(cpu_count()) as p:
        raw_output = p.map(analyze_fund, csv_files)
    
    analysis_results = [r['analysis'] for r in raw_output if r and r['analysis']['代码'] in name_map]
    hist_data_map = {r['analysis']['代码']: r['history'] for r in raw_output if r}

    # --- 归档存储逻辑 ---
    # 筛选有建议的品种：买入在前（按RSI从低到高），卖出在后
    buy_list = sorted([r for r in analysis_results if r['信号'] == "建议买入"], key=lambda x: x['RSI'])
    sell_list = sorted([r for r in analysis_results if r['信号'] == "建议卖出"], key=lambda x: x['RSI'], reverse=True)
    all_decisions = buy_list + sell_list

    now = datetime.now()
    # 自动生成年月文件夹：results/2026/01/
    archive_dir = os.path.join(BASE_RESULT_DIR, now.strftime('%Y'), now.strftime('%m'))
    os.makedirs(archive_dir, exist_ok=True)
    archive_path = os.path.join(archive_dir, f"scan_{now.strftime('%Y%m%d')}.csv")

    if all_decisions:
        out_df = pd.DataFrame(all_decisions)
        out_df.insert(2, '简称', out_df['代码'].apply(lambda x: name_map.get(x, '未知')))
        out_df.to_csv(archive_path, index=False, encoding='utf-8-sig')
        print(f"💾 官方决策报告已持久化存至: {archive_path}")

    # --- 刷新胜率回测账本 ---
    tracker_df = update_tracker(analysis_results, hist_data_map)

    # --- 控制台可视化输出 ---
    print(f"\n📅 分析报告日期: {now.strftime('%Y-%m-%d')}")
    print("=" * 90)
    print(f"{'代码':<8} | {'简称':<12} | {'价格':<7} | {'RSI':<5} | {'偏离度%':<8} | {'建议决策'}")
    print("-" * 90)
    
    if not all_decisions:
        print("   (当前市场情绪稳定，无极端买卖建议，建议现有网格策略正常运行)")
    else:
        for r in buy_list:
            print(f"🟢 {r['代码']:<6} | {name_map[r['代码']]:<10} | {r['价格']:<7} | {r['RSI']:<5} | {r['偏离度%']:<8} | {r['信号']}({r['理由']})")
        for r in sell_list:
            print(f"🔴 {r['代码']:<6} | {name_map[r['代码']]:<10} | {r['价格']:<7} | {r['RSI']:<5} | {r['偏离度%']:<8} | {r['信号']}({r['理由']})")
    print("=" * 90)

    # --- 胜率实时简报 ---
    print("\n📈 历史信号胜率实时监控 (基于 signal_tracker.csv):")
    for t in [7, 14, 20, 60]:
        col = f'T+{t}收益%'
        if col in tracker_df.columns:
            valid_rows = tracker_df[tracker_df[col].notna()]
            if not valid_rows.empty:
                win_rate = (valid_rows[col].astype(float) > 0).sum() / len(valid_rows) * 100
                avg_ret = valid_rows[col].astype(float).mean()
                print(f" >> 买入{t:2d}天后: 胜率 {win_rate:5.1f}% | 平均收益 {avg_ret:5.2f}% (样本数:{len(valid_rows)})")
            else:
                print(f" >> 买入{t:2d}天后: 样本数据积累中...")

if __name__ == "__main__":
    main()

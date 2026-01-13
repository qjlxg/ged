import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：Alpha Hunter V5 终极实战验证版
# 核心功能：
# 1. [全功能逻辑] 保留V3所有量价、趋势、动态ATR、横盘判定逻辑
# 2. [模拟实盘] 出现信号当天自动“虚拟买入”，记录收盘价
# 3. [胜率追踪] 自动刷新并计算 T+7, T+14, T+20, T+60 的真实收益与胜率
# 4. [持久化账本] 结果存入 signal_tracker.csv，随日期推移自动更新历史表现
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 
TRACKER_FILE = 'signal_tracker.csv' # 模拟买入记录账本

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def analyze_fund(file_path):
    try:
        # 为了回测和MA60，读取足够长的数据
        full_df = pd.read_csv(file_path, encoding='utf-8-sig')
        if len(full_df) < 60: return None
        full_df.columns = [c.strip() for c in full_df.columns]
        
        # 截取计算用的片段
        df = full_df.tail(120).copy()
        latest = df.iloc[-1]
        close_series = df['收盘']
        vol_series = df['成交额']
        
        # --- [指标计算] ---
        ma20_s = close_series.rolling(20).mean()
        ma60_s = close_series.rolling(60).mean()
        ma20, ma60 = ma20_s.iloc[-1], ma60_s.iloc[-1]
        rsi_val = calculate_rsi(close_series).iloc[-1]
        bias = (latest['收盘'] - ma20) / ma20 * 100
        
        high_low = df['最高'] - df['最低']
        atr = high_low.rolling(14).mean().iloc[-1]
        relative_atr = (atr / latest['收盘']) * 100
        vol_ratio = vol_series.tail(5).mean() / (vol_series.tail(20).mean() + 1e-9)

        # --- [逻辑判定] ---
        trend_status = "多头排列" if ma20 > ma60 else "空头排列"
        dynamic_threshold = max(0.018, relative_atr * 0.5 / 100)
        is_sideways = ((close_series - ma20_s) / ma20_s).abs() < dynamic_threshold
        sideways_days = 0
        for val in reversed(is_sideways.values):
            if val: sideways_days += 1
            else: break
            
        is_divergence = (latest['收盘'] > ma20) and (vol_ratio < 0.8)
        
        status, action, weight, star = "正常震荡", "常规网格", "1.0x", "★★★☆☆"
        is_buy_signal = False # 是否触发记录

        if rsi_val < 38:
            status, star, is_buy_signal = "🔥机会区", "★★★★☆", True
            if rsi_val < 32:
                status, action, weight = "🚨超卖加码", "暂停卖出/积极买入", "1.5x"
                if vol_ratio > 1.15 and bias < -4.5:
                    status, star, action, weight = "💎五星金底", "★★★★★", "全力买入/持有", "2.0x"
        elif is_divergence:
            status, action, star = "🚫缩量诱多", "停止买入/仅卖出", "★★☆☆☆"

        code = os.path.basename(file_path).replace('.csv', '')
        
        # 返回分析数据用于当日展示，返回全量历史用于回测更新
        return {
            'analysis': {
                '证券代码': code, '收盘价': latest['收盘'], 'RSI(14)': round(rsi_val, 2),
                '趋势': trend_status, '乖离率%': round(bias, 2), '量能比': round(vol_ratio, 2),
                '波动率%': round(relative_atr, 2), '横盘天数': sideways_days, '网格状态': status,
                '胜率置信度': star, '建议操作': action, '加码倍数': weight, 'is_signal': is_buy_signal,
                'current_date': latest['日期'] if '日期' in latest else datetime.now().strftime('%Y-%m-%d')
            },
            'history': full_df[['日期', '收盘']]
        }
    except Exception: return None

def process_backtest(new_data_list, all_hist_map):
    """更新信号追踪账本"""
    if os.path.exists(TRACKER_FILE):
        tracker = pd.read_csv(TRACKER_FILE)
    else:
        tracker = pd.DataFrame(columns=['代码', '买入日期', '买入价', 'T+7收益%', 'T+14收益%', 'T+20收益%', 'T+60收益%', '状态'])

    # 1. 记录今日新信号
    for item in new_data_list:
        if item['is_signal']:
            # 同一品种10天内不重复记录买入信号，防止信号刷屏
            recent = tracker[(tracker['代码'] == item['证券代码'])].tail(1)
            if recent.empty or (datetime.now() - pd.to_datetime(recent['买入日期'].values[0])).days > 10:
                new_row = pd.DataFrame([{
                    '代码': item['证券代码'], '买入日期': item['current_date'], '买入价': item['收盘价'],
                    'T+7收益%': np.nan, 'T+14收益%': np.nan, 'T+20收益%': np.nan, 'T+60收益%': np.nan, '状态': '持有中'
                }])
                tracker = pd.concat([tracker, new_row], ignore_index=True)

    # 2. 遍历账本，用最新历史数据刷新收益
    for idx, row in tracker.iterrows():
        code = str(row['代码']).zfill(6)
        if code in all_hist_map:
            h_df = all_hist_map[code].copy()
            h_df['日期'] = pd.to_datetime(h_df['日期'])
            buy_date = pd.to_datetime(row['买入日期'])
            
            # 获取买入日之后的所有数据
            future_prices = h_df[h_df['日期'] > buy_date].copy()
            if not future_prices.empty:
                for t in [7, 14, 20, 60]:
                    col = f'T+{t}收益%'
                    if pd.isna(row[col]) and len(future_prices) >= t:
                        p_t = future_prices.iloc[t-1]['收盘']
                        tracker.at[idx, col] = round((p_t - row['买入价']) / row['买入价'] * 100, 2)
                
                if len(future_prices) >= 60:
                    tracker.at[idx, '状态'] = '已结项'

    tracker.to_csv(TRACKER_FILE, index=False, encoding='utf-8-sig')
    return tracker

def main():
    # --- 加载列表 ---
    target_file = None
    for f in [ETF_LIST_FILE, ETF_LIST_FILE.replace('.xlsx', '.csv')]:
        if os.path.exists(f): target_file = f; break
    if not target_file: return

    try:
        if target_file.endswith('.xlsx'): name_df = pd.read_excel(target_file, engine='openpyxl')
        else: name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df.columns = [c.strip() for c in name_df.columns]
        name_df['证券代码'] = name_df['证券代码'].astype(str).str.zfill(6)
        name_map = dict(zip(name_df['证券代码'], name_df['证券简称']))
    except: return

    # --- 并行执行 ---
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🚀 Alpha Hunter V5 启动... 正在分析并回测 {len(csv_files)} 个品种")
    with Pool(cpu_count()) as p:
        raw_results = p.map(analyze_fund, csv_files)
    
    analysis_results = [r['analysis'] for r in raw_results if r and r['analysis']['证券代码'] in name_map]
    hist_map = {r['analysis']['证券代码']: r['history'] for r in raw_results if r}
    
    if not analysis_results: return

    # --- 更新账本与胜率统计 ---
    tracker_df = process_backtest(analysis_results, hist_map)
    
    # --- 输出报表 ---
    final_df = pd.DataFrame(analysis_results)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    cols = ['证券代码', '证券简称', '收盘价', 'RSI(14)', '趋势', '乖离率%', '量能比', '网格状态', '胜率置信度', '建议操作']
    final_df = final_df[cols].sort_values(by=['胜率置信度', 'RSI(14)'], ascending=[False, True])

    # 保存今日扫描结果
    now = datetime.now()
    os.makedirs(now.strftime('%Y/%m'), exist_ok=True)
    save_path = os.path.join(now.strftime('%Y/%m'), f"alpha_v5_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')

    print(f"\n✅ 今日扫描完成: {save_path}")
    print("-" * 100)
    print(final_df.head(10))

    # 打印胜率简报
    print("\n📈 历史信号胜率验证 (signal_tracker.csv):")
    for t in [7, 14, 20, 60]:
        col = f'T+{t}收益%'
        valid = tracker_df[tracker_df[col].notna()]
        if not valid.empty:
            wr = (valid[col] > 0).sum() / len(valid) * 100
            avg = valid[col].mean()
            print(f" >> {col}: 样本数 {len(valid)}, 胜率 {wr:.1f}%, 平均收益 {avg:.2f}%")
        else:
            print(f" >> {col}: 样本不足，等待后续数据刷新...")

if __name__ == "__main__":
    main()

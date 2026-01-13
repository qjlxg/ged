import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 脚本说明：Alpha Hunter 终极实战版
# 核心逻辑：
# 1. [情绪监测] 监测价格是否涨过头（太烫）或跌透了（太冰）。
# 2. [人气检测] 价格涨但没人跟（虚假繁荣）时自动报警。
# 3. [自动记账] 发现好机会自动记入“signal_tracker.csv”，帮你算后续涨跌。
# 4. [环境判定] 区分现在是“顺风局”（多头）还是“逆风局”（空头）。
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 
TRACKER_FILE = 'signal_tracker.csv' # 你的模拟持仓小账本

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
        close_series = df['收盘']
        vol_series = df['成交额']
        
        # --- [计算核心指标] ---
        ma20_s = close_series.rolling(20).mean()
        ma60_s = close_series.rolling(60).mean()
        ma20, ma60 = ma20_s.iloc[-1], ma60_s.iloc[-1]
        
        # 价格离家（均线）的远近
        dist_pct = (latest['收盘'] - ma20) / ma20 * 100
        # 情绪温度 (RSI)
        temp = calculate_rsi(close_series).iloc[-1]
        
        # 活跃度 (量能比)
        pop = vol_series.tail(5).mean() / (vol_series.tail(20).mean() + 1e-9)
        
        # 波动弹性 (ATR)
        high_low = df['最高'] - df['最低']
        flex = (high_low.rolling(14).mean().iloc[-1] / latest['收盘']) * 100

        # --- [逻辑判定] ---
        # 环境判断
        env = "顺风局(强)" if ma20 > ma60 else "逆风局(弱)"
        
        # 横盘磨洋工判定
        sideways_limit = max(0.018, flex * 0.5 / 100)
        is_boring = ((close_series - ma20_s) / ma20_s).abs() < sideways_limit
        boring_days = 0
        for val in reversed(is_boring.values):
            if val: boring_days += 1
            else: break
            
        # 虚假繁荣判定 (涨了但没人气)
        fake_up = (latest['收盘'] > ma20) and (pop < 0.8)
        
        # 初始结论
        desc, act, multi, star = "正常波动", "该买买该卖卖", "1.0x", "★★★☆☆"
        is_signal = False 

        # A. 抄底逻辑 (大白话版)
        if temp < 38:
            desc, star, is_signal = "🔥跌透了", "★★★★☆", True
            act = "可以分批买"
            if temp < 32:
                desc, act, multi = "🚨极度冰点", "只买不卖/大胆加仓", "1.5x"
                if pop > 1.15 and dist_pct < -4.5:
                    desc, star, act, multi = "💎黄金坑", "★★★★★", "全力捡钱", "2.0x"
        
        # B. 风险逻辑
        elif temp > 70 or fake_up:
            star = "★★☆☆☆"
            if fake_up:
                desc, act = "🚫虚假繁荣", "别追！小心被套"
            else:
                desc, act = "⚠️太烫了", "见好就收/分批离场"
        
        # C. 突破逻辑
        elif env == "顺风局(强)" and 0 < dist_pct < 2.5 and boring_days >= 4:
            desc, star, act = "🚀要起飞", "★★★★☆", "拿稳了等涨"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            'analysis': {
                '代码': code, '现价': latest['收盘'], '情绪温度': round(temp, 1),
                '当前环境': env, '离家距离%': round(dist_pct, 2), '人气值': round(pop, 2),
                '波动弹性%': round(flex, 2), '磨洋工天数': boring_days, '市场诊断': desc,
                '置信度': star, '操作建议': act, '加仓倍数': multi, 'is_signal': is_signal,
                'date': latest['日期'] if '日期' in latest else datetime.now().strftime('%Y-%m-%d')
            },
            'history': full_df[['日期', '收盘']]
        }
    except Exception: return None

def process_backtest(new_data_list, all_hist_map):
    if os.path.exists(TRACKER_FILE):
        tracker = pd.read_csv(TRACKER_FILE)
    else:
        tracker = pd.DataFrame(columns=['代码', '入场日期', '买入价', '7天后收益%', '14天后收益%', '20天后收益%', '60天后收益%', '状态'])

    for item in new_data_list:
        if item['is_signal']:
            recent = tracker[(tracker['代码'] == item['代码'])].tail(1)
            if recent.empty or (datetime.now() - pd.to_datetime(recent['入场日期'].values[0])).days > 10:
                new_row = pd.DataFrame([{
                    '代码': item['代码'], '入场日期': item['date'], '买入价': item['现价'],
                    '7天后收益%': np.nan, '14天后收益%': np.nan, '20天后收益%': np.nan, '60天后收益%': np.nan, '状态': '持有中'
                }])
                tracker = pd.concat([tracker, new_row], ignore_index=True)

    for idx, row in tracker.iterrows():
        code = str(row['代码']).zfill(6)
        if code in all_hist_map:
            h_df = all_hist_map[code].copy()
            h_df['日期'] = pd.to_datetime(h_df['日期'])
            buy_dt = pd.to_datetime(row['入场日期'])
            future = h_df[h_df['日期'] > buy_dt].copy()
            if not future.empty:
                for t in [7, 14, 20, 60]:
                    col = f'{t}天后收益%'
                    if pd.isna(row[col]) and len(future) >= t:
                        p_t = future.iloc[t-1]['收盘']
                        tracker.at[idx, col] = round((p_t - row['买入价']) / row['买入价'] * 100, 2)
                if len(future) >= 60: tracker.at[idx, '状态'] = '已结项'

    tracker.to_csv(TRACKER_FILE, index=False, encoding='utf-8-sig')
    return tracker

def main():
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

    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🚀 Alpha Hunter 启动：正在帮你看管 {len(csv_files)} 个品种...")
    with Pool(cpu_count()) as p:
        raw_results = p.map(analyze_fund, csv_files)
    
    analysis_results = [r['analysis'] for r in raw_results if r and r['analysis']['代码'] in name_map]
    hist_map = {r['analysis']['代码']: r['history'] for r in raw_results if r}
    
    if not analysis_results: return
    tracker_df = process_backtest(analysis_results, hist_map)
    
    final_df = pd.DataFrame(analysis_results)
    final_df['简称'] = final_df['代码'].apply(lambda x: name_map[x])
    cols = ['代码', '简称', '现价', '情绪温度', '当前环境', '市场诊断', '置信度', '操作建议', '人气值', '磨洋工天数']
    final_df = final_df[cols].sort_values(by=['置信度', '情绪温度'], ascending=[False, True])

    now = datetime.now()
    os.makedirs(now.strftime('%Y/%m'), exist_ok=True)
    save_path = os.path.join(now.strftime('%Y/%m'), f"market_scan_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')

    print(f"\n✅ 诊断报告已生成：{save_path}")
    print("-" * 90)
    print(final_df.head(10))

    print("\n📈 历史“买入”后的表现验证 (帮你测试这套方法灵不灵):")
    for t in [7, 14, 20, 60]:
        col = f'{t}天后收益%'
        valid = tracker_df[tracker_df[col].notna()]
        if not valid.empty:
            wr = (valid[col] > 0).sum() / len(valid) * 100
            print(f" >> 买入{t}天后：成功率 {wr:.1f}%, 平均赚 {valid[col].mean():.2f}% (样本:{len(valid)}个)")
        else:
            print(f" >> 买入{t}天后：还在观察中，过几天再来看...")

if __name__ == "__main__":
    main()

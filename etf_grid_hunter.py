import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 脚本说明：Alpha Hunter 终极实战版 (V5.1 稳定版)
# 修复：解决首次运行账本列名匹配导致的 KeyError 报错
# 功能：全自动量价扫描 + 历史胜率记账 + 大白话诊断
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
        close_series = df['收盘']
        vol_series = df['成交额']
        
        ma20_s = close_series.rolling(20).mean()
        ma60_s = close_series.rolling(60).mean()
        ma20, ma60 = ma20_s.iloc[-1], ma60_s.iloc[-1]
        
        dist_pct = (latest['收盘'] - ma20) / ma20 * 100
        temp = calculate_rsi(close_series).iloc[-1]
        pop = vol_series.tail(5).mean() / (vol_series.tail(20).mean() + 1e-9)
        
        high_low = df['最高'] - df['最低']
        flex = (high_low.rolling(14).mean().iloc[-1] / latest['收盘']) * 100

        env = "顺风局(强)" if ma20 > ma60 else "逆风局(弱)"
        
        sideways_limit = max(0.018, flex * 0.5 / 100)
        is_boring = ((close_series - ma20_s) / ma20_s).abs() < sideways_limit
        boring_days = 0
        for val in reversed(is_boring.values):
            if val: boring_days += 1
            else: break
            
        fake_up = (latest['收盘'] > ma20) and (pop < 0.8)
        
        desc, act, multi, star = "正常波动", "该买买该卖卖", "1.0x", "★★★☆☆"
        is_signal = False 

        if temp < 38:
            desc, star, is_signal = "🔥跌透了", "★★★★☆", True
            act = "可以分批买"
            if temp < 32:
                desc, act, multi = "🚨极度冰点", "只买不卖/大胆加仓", "1.5x"
                if pop > 1.15 and dist_pct < -4.5:
                    desc, star, act, multi = "💎黄金坑", "★★★★★", "全力捡钱", "2.0x"
        elif temp > 70 or fake_up:
            star = "★★☆☆☆"
            if fake_up:
                desc, act = "🚫虚假繁荣", "别追！小心被套"
            else:
                desc, act = "⚠️太烫了", "见好就收/分批离场"
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
    # 定义列名模板，确保后续访问一致
    columns = ['代码', '入场日期', '买入价', '7天后收益%', '14天后收益%', '20天后收益%', '60天后收益%', '状态']
    
    if os.path.exists(TRACKER_FILE):
        try:
            tracker = pd.read_csv(TRACKER_FILE)
            # 确保列名一致性
            for col in columns:
                if col not in tracker.columns: tracker[col] = np.nan
        except:
            tracker = pd.DataFrame(columns=columns)
    else:
        tracker = pd.DataFrame(columns=columns)

    # 1. 记录今日新信号
    for item in new_data_list:
        if item['is_signal']:
            recent = tracker[(tracker['代码'] == item['代码'])].tail(1)
            # 10天内不重复记录同一品种
            can_add = True
            if not recent.empty:
                last_date = pd.to_datetime(recent['入场日期'].values[0])
                if (datetime.now() - last_date).days < 10:
                    can_add = False
            
            if can_add:
                new_row = pd.DataFrame([{c: np.nan for c in columns}])
                new_row['代码'] = item['代码']
                new_row['入场日期'] = item['date']
                new_row['买入价'] = item['现价']
                new_row['状态'] = '持有中'
                tracker = pd.concat([tracker, new_row], ignore_index=True)

    # 2. 刷新收益率
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
    print(f"🚀 Alpha Hunter 启动：扫描 {len(csv_files)} 个品种...")
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
    print("-" * 100)
    print(final_df.head(10))

    print("\n📈 历史“买入”后的表现验证:")
    # 增加健壮性检查，确保列存在
    for t in [7, 14, 20, 60]:
        col = f'{t}天后收益%'
        if col in tracker_df.columns:
            # 显式转换为数值类型，避免 object 类型导致 notna() 异常
            valid_vals = pd.to_numeric(tracker_df[col], errors='coerce')
            valid = tracker_df[valid_vals.notna()]
            if not valid.empty:
                wr = (pd.to_numeric(valid[col]) > 0).sum() / len(valid) * 100
                avg = pd.to_numeric(valid[col]).mean()
                print(f" >> 买入{t:2d}天后：成功率 {wr:5.1f}%, 平均赚 {avg:5.2f}% (样本:{len(valid)}个)")
            else:
                print(f" >> 买入{t:2d}天后：还在观察中...")
        else:
            print(f" >> 买入{t:2d}天后：列名异常")

if __name__ == "__main__":
    main()

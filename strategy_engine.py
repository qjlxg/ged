import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool, cpu_count

# ==========================================
# --- 1. 核心锁死风控与策略参数 ---
# ==========================================
TOTAL_BUDGET_CAP = 10000   # 1万本金总上限
PORTFOLIO_UNIT = 2000      # 单笔抄底金额
STOP_BUY_LOSS_RATIO = -5.0 # 组合总亏损超5%，开启禁买令
RETR_WATCH = -15.0         # 10%回调介入
RETR_WINDOW = 250          # 250日实战周期
LIQUIDITY_LIMIT = 10000000 # 日均成交额低于1000万不入池

# ==========================================
# --- 2. 映射逻辑：加载 ETF 名称 ---
# ==========================================
def load_name_mapping():
    mapping = {}
    try:
        if os.path.exists('ETF列表.txt'):
            try:
                df_map = pd.read_csv('ETF列表.txt', sep='\t', dtype={'证券代码': str})
            except:
                df_map = pd.read_csv('ETF列表.txt', sep='\t', dtype={'证券代码': str}, encoding='gbk')
            for _, row in df_map.iterrows():
                code = str(row['证券代码']).zfill(6)
                mapping[code] = row['证券简称']
    except: pass
    return mapping

NAME_MAP = load_name_mapping()

# ==========================================
# --- 3. 技术指标模块 (双RSI + BIAS) ---
# ==========================================
def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs.fillna(0)))

def check_strong_divergence(df, window=20):
    if len(df) < window + 5: return False
    curr_p = df['收盘'].iloc[-1]
    curr_rsi = df['rsi6'].iloc[-1]
    lookback = df.iloc[-(window+1):-1]
    min_idx = lookback['收盘'].idxmin()
    min_p = lookback.loc[min_idx, '收盘']
    min_rsi = lookback.loc[min_idx, 'rsi6']
    if curr_p < min_p * 0.99 and curr_rsi > min_rsi + 5:
        return True
    return False

# ==========================================
# --- 4. 单文件处理 (核心扫描逻辑) ---
# ==========================================
def process_file(file_path):
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        if 'net_value' in df.columns: df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
        if '日期' not in df.columns or '收盘' not in df.columns: return None

        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)
        if len(df) < 60: return None 
        
        if '成交额' in df.columns and df['成交额'].iloc[-5:].mean() < LIQUIDITY_LIMIT: return None

        df['rsi6'] = calculate_rsi(df['收盘'], 6)
        df['rsi14'] = calculate_rsi(df['收盘'], 14)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        df['bias20'] = ((df['收盘'] - df['ma20']) / df['ma20']) * 100
        df['max_high'] = df['收盘'].rolling(window=RETR_WINDOW).max()
        df['retr'] = ((df['收盘'] - df['max_high']) / df['max_high']) * 100
        
        df['in_watch'] = df['retr'] <= RETR_WATCH
        df['persist_days'] = df['in_watch'].groupby((df['in_watch'] != df['in_watch'].shift()).cumsum()).cumcount() + 1
        df.loc[~df['in_watch'], 'persist_days'] = 0

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0].zfill(6)
        
        if curr['in_watch']:
            score = 1
            divergence = check_strong_divergence(df)
            if curr['rsi6'] < 30 and curr['rsi14'] < 45: score += 2
            if curr['bias20'] < -7: score += 2
            if divergence: score += 2
            
            risk_level = "正常"
            if divergence: risk_level = "📈底背离"
            elif curr['rsi6'] > 60: risk_level = "🚩假摔(慎入)"
                
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                '名称': NAME_MAP.get(code, "未知"),
                '评分': score,
                '持续天数': int(curr['persist_days']),
                '风险预警': risk_level,
                '回撤%': round(curr['retr'], 2),
                'RSI6': round(curr['rsi6'], 2),
                'BIAS20': round(curr['bias20'], 2),
                'price': round(curr['收盘'], 4)
            }
    except: return None

# ==========================================
# --- 5. 盈亏统计 (去重清爽版) ---
# ==========================================
def get_performance_stats():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    raw_signals = []
    for h_file in history_files:
        if 'perf' in h_file: continue
        try: raw_signals.append(pd.read_csv(h_file))
        except: continue
    
    if not raw_signals: return pd.DataFrame()
    
    # 核心：按代码去重，只保留最早的那次信号作为“初始建仓点”
    all_sig = pd.concat(raw_signals).sort_values('date')
    first_signals = all_sig.drop_duplicates(subset=['fund_code'], keep='first')
    
    perf_list = []
    for _, sig in first_signals.iterrows():
        code = str(sig['fund_code']).zfill(6)
        raw_path = f'fund_data/{code}.csv'
        if not os.path.exists(raw_path): continue
        
        try:
            raw_df = pd.read_csv(raw_path)
            if 'net_value' in raw_df.columns: raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
            
            # 计算趋势线：5日线和10日线
            raw_df['ma5'] = raw_df['收盘'].rolling(window=5).mean()
            raw_df['ma10'] = raw_df['收盘'].rolling(window=10).mean()
            
            latest = raw_df.iloc[-1]
            entry_price = sig['price']
            
            # 仅追踪最近30天内的信号，避免列表过长
            if (datetime.now() - pd.to_datetime(sig['date'])).days > 30: continue
            
            # 计算最高浮盈和当前盈亏
            # 找到信号日之后的最高价
            raw_df['日期_str'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
            after_signal_df = raw_df[raw_df['日期_str'] >= str(sig['date'])]
            max_p = after_signal_df['收盘'].max()
            
            max_profit = (max_p - entry_price) / entry_price * 100
            current_profit = (latest['收盘'] - entry_price) / entry_price * 100
            is_dead_cross = latest['ma5'] < latest['ma10'] and len(after_signal_df) > 3

            perf_list.append({
                '建仓日期': sig['date'], '代码': code, '名称': NAME_MAP.get(code, "未知"),
                '建仓价': round(entry_price, 4), '最新价': round(latest['收盘'], 4),
                '最高浮盈%': round(max_profit, 2), '当前盈亏%': round(current_profit, 2),
                '死叉': "YES" if is_dead_cross else "NO",
                '状态': "✅趋势向上" if not is_dead_cross else "🚨趋势走弱"
            })
        except: continue
    return pd.DataFrame(perf_list)

# ==========================================
# --- 6. 报告生成 ---
# ==========================================
def update_readme(current_res, perf_df):
    now_bj = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 (清爽版)\n\n> 最后更新: `{now_bj}`\n\n"
    content += "### 🚩 止盈逻辑说明：\n- **趋势持仓**：5日均线在10日均线上方时安心持有。\n- **强制离场**：(浮盈>5% 且 出现均线死叉) 或 (利润较最高点回吐3%)。\n\n"
    
    if not perf_df.empty:
        total_p = (perf_df['当前盈亏%'] / 100 * PORTFOLIO_UNIT).sum()
        content += f"## 💰 账户实战概览\n> **总盈亏估算**: `¥{total_p:.2f}` | **状态**: `{'🛡️安全运营' if total_p > -500 else '❌触发禁买'}`\n\n"

    content += "## 🎯 实时扫描信号\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        content += df.to_markdown(index=False) + "\n\n"

    content += "## 🔥 活跃品种追踪 (不重复显示)\n"
    if not perf_df.empty:
        def decide(row):
            if row['当前盈亏%'] >= 5.0 and row['死叉'] == "YES": return "🚨 死叉止盈"
            if row['最高浮盈%'] > 5.0 and row['当前盈亏%'] < (row['最高浮盈%'] - 3.0): return "🚨 回吐止盈"
            return row['状态']
        
        perf_df['操作建议'] = perf_df.apply(decide, axis=1)
        content += perf_df[['建仓日期', '代码', '名称', '建仓价', '最新价', '最高浮盈%', '当前盈亏%', '操作建议']].to_markdown(index=False) + "\n\n"

    with open('README.md', 'w', encoding='utf-8') as f: f.write(content)

# ==========================================
# --- 7. 主程序 ---
# ==========================================
def main():
    if not os.path.exists('fund_data'): return
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    
    if results:
        now = datetime.now()
        folder = now.strftime('%Y/%m')
        os.makedirs(folder, exist_ok=True)
        pd.DataFrame(results).to_csv(f"{folder}/sig_{now.strftime('%d_%H%M%S')}.csv", index=False)
    
    update_readme(results, get_performance_stats())

if __name__ == "__main__":
    main()
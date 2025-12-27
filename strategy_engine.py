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
RETR_WATCH = -10.0         # 10%回调介入
RETR_WINDOW = 250          # 250日实战周期
LIQUIDITY_LIMIT = 10000000 # 日均成交额低于1000万不入池

# ==========================================
# --- 2. 映射逻辑：加载 ETF 名称 ---
# ==========================================
def load_name_mapping():
    mapping = {}
    try:
        if os.path.exists('ETF列表.txt'):
            for enc in ['utf-8', 'gbk']:
                try:
                    df_map = pd.read_csv('ETF列表.txt', sep='\t', dtype={'证券代码': str}, encoding=enc)
                    for _, row in df_map.iterrows():
                        code = str(row['证券代码']).zfill(6)
                        mapping[code] = row['证券简称']
                    break
                except: continue
    except Exception as e:
        print(f"名称映射加载失败: {e}")
    return mapping

NAME_MAP = load_name_mapping()

# ==========================================
# --- 3. 增强版技术指标模块 ---
# ==========================================
def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs.fillna(0)))

def check_strong_divergence(df, window=20):
    """价格创新低但RSI回升：强力底背离判断"""
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
# --- 4. 单文件处理 (算法增强扫描) ---
# ==========================================
def process_file(file_path):
    try:
        df = None
        for enc in ['utf-8', 'gbk']:
            try: df = pd.read_csv(file_path, encoding=enc); break
            except: continue
        if df is None: return None
        
        if 'net_value' in df.columns: df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
        if '日期' not in df.columns or '收盘' not in df.columns: return None

        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)
        if len(df) < 60: return None 
        
        if '成交额' in df.columns and df['成交额'].iloc[-5:].mean() < LIQUIDITY_LIMIT: return None

        # 计算增强指标
        df['rsi6'] = calculate_rsi(df['收盘'], 6)
        df['rsi14'] = calculate_rsi(df['收盘'], 14)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        df['bias6'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
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
            if curr['rsi6'] < 30 and curr['rsi14'] < 40: score += 2
            if curr['bias6'] < -5 and curr['bias20'] < -7: score += 2
            if divergence: score += 2
            
            risk_level = "正常"
            if divergence: risk_level = "📈强力底背离"
            elif curr['rsi6'] > 50: risk_level = "🚩假摔陷阱"
                
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                '名称': NAME_MAP.get(code, "未知品种"),
                '评分': score,
                '风险预警': risk_level,
                '回撤%': round(curr['retr'], 2),
                'RSI6': round(curr['rsi6'], 2),
                'BIAS20': round(curr['bias20'], 2),
                'price': round(curr['收盘'], 4),
                'ma5_trend': "UP" if curr['ma6'] > df['ma6'].iloc[-2] else "DOWN"
            }
    except: return None

# ==========================================
# --- 5. 盈亏统计 (修正乱码与前几天记录丢失逻辑) ---
# ==========================================
def get_performance_stats():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    temp_dfs = []
    
    # 兼容性读取：统一列名，防止乱码和NaN
    for f in history_files:
        if 'perf' in f: continue
        try:
            df = pd.read_csv(f)
            # 兼容旧列名
            rename_map = {'RSI': 'RSI6', 'BIAS': 'BIAS20'}
            df = df.rename(columns=rename_map)
            # 只提取核心对齐列，丢弃可能引起乱码的杂列
            keep_cols = [c for c in ['date', 'fund_code', 'price', '评分'] if c in df.columns]
            temp_dfs.append(df[keep_cols])
        except: continue
    
    if not temp_dfs: return pd.DataFrame()
    
    # 合并所有历史信号并按日期排序
    full_history = pd.concat(temp_dfs).sort_values('date')
    

    unique_signals = full_history[full_history['评分'] >= 3].drop_duplicates(subset=['fund_code'], keep='first')
    
    perf_list = []
    for _, sig in unique_signals.iterrows():
        code = str(sig['fund_code']).zfill(6)
        raw_path = f'fund_data/{code}.csv'
        if not os.path.exists(raw_path): continue
        try:
            raw_df = pd.read_csv(raw_path)
            if 'net_value' in raw_df.columns: raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
            raw_df['日期_str'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
            
            # 找到信号日及之后的数据
            after_df = raw_df[raw_df['日期_str'] >= str(sig['date'])].copy()
            if after_df.empty: continue
            
            # 计算止盈均线
            after_df['ma5'] = after_df['收盘'].rolling(window=5).mean()
            after_df['ma10'] = after_df['收盘'].rolling(window=10).mean()
            
            latest = after_df.iloc[-1]
            entry_p = sig['price']
            max_p = after_df['收盘'].max()
            
            total_change = (latest['收盘'] - entry_p) / entry_p * 100
            max_profit = (max_p - entry_p) / entry_p * 100
            is_dead = latest['ma5'] < latest['ma10'] and len(after_df) > 3

            perf_list.append({
                '日期': sig['date'], '代码': code, '名称': NAME_MAP.get(code, "未知"),
                '评分': sig.get('评分', 1), '最新价': round(latest['收盘'], 4), 
                '最高浮盈%': round(max_profit, 2), '总盈亏%': round(total_change, 2),
                '状态': "✅趋势向上" if not is_dead else "🚨趋势走弱",
                '死叉': "YES" if is_dead else "NO"
            })
        except: continue
    return pd.DataFrame(perf_list)

# ==========================================
# --- 6. 最终决策报告生成 ---
# ==========================================
def update_readme(current_res, perf_df):
    now_bj = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 (全功能完整版)\n\n> 最后更新: `{now_bj}`\n\n"
    content += "### 🚩 战法铁律：\n- **双RSI共振**：RSI6 < 30 且 RSI14 < 40 确认为真底部。\n- **趋势止盈**：浮盈 > 5% 后，出现 **均线死叉** 或 **利润回吐3%** 强制离场。\n\n"
    
    if not perf_df.empty:
        total_p = (perf_df['总盈亏%'] / 100 * PORTFOLIO_UNIT).sum()
        avg_r = (perf_df['总盈亏%'].mean()) if len(perf_df)>0 else 0
        content += f"## 💰 实战风控盘口\n> **总盈亏**: `¥{total_p:.2f} ({avg_r:+.2f}%)` | **风控**: `{'🛡️安全' if avg_r > STOP_BUY_LOSS_RATIO else '❌停买'}`\n\n"

    content += "## 🎯 实时信号 (双指标共振版)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        content += df[['date', 'fund_code', '名称', '评分', '风险预警', '回撤%', 'RSI6', 'BIAS20', 'price']].to_markdown(index=False) + "\n\n"

    content += "## 🔥 活跃买点 (历史兼容去重)\n"
    if not perf_df.empty:
        def decide_sell(row):
            if row['总盈亏%'] >= 5.0 and row['死叉'] == "YES": return "🚨 趋势反转，清仓！"
            if row['最高浮盈%'] > 5.0 and row['总盈亏%'] < (row['最高浮盈%'] - 3.0): return "🚨 利润回吐，结账"
            return row['状态']
        
        perf_df['操作建议'] = perf_df.apply(decide_sell, axis=1)
        # 确保按日期降序排列，新机会在上面
        perf_df = perf_df.sort_values('日期', ascending=False)
        content += perf_df[['日期', '代码', '名称', '评分', '最高浮盈%', '总盈亏%', '操作建议']].to_markdown(index=False) + "\n\n"

    with open('README.md', 'w', encoding='utf-8') as f: f.write(content)

# ==========================================
# --- 7. 主程序入口 ---
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
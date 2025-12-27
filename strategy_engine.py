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
            try:
                df_map = pd.read_csv('ETF列表.txt', sep='\t', dtype={'证券代码': str})
            except:
                df_map = pd.read_csv('ETF列表.txt', sep='\t', dtype={'证券代码': str}, encoding='gbk')
            for _, row in df_map.iterrows():
                code = str(row['证券代码']).zfill(6)
                mapping[code] = row['证券简称']
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
    """
    增强版底背离：价格创新低（且比前低至少低1%），RSI显著回升
    """
    if len(df) < window + 5: return False
    curr_p = df['收盘'].iloc[-1]
    curr_rsi = df['rsi6'].iloc[-1]
    
    lookback = df.iloc[-(window+1):-1]
    min_idx = lookback['收盘'].idxmin()
    min_p = lookback.loc[min_idx, '收盘']
    min_rsi = lookback.loc[min_idx, 'rsi6']
    
    # 逻辑过滤：当前价必须跌破前低，且RSI比前低点时的RSI高出5点以上
    if curr_p < min_p * 0.99 and curr_rsi > min_rsi + 5:
        return True
    return False

# ==========================================
# --- 4. 单文件处理 (双指标共振版) ---
# ==========================================
def process_file(file_path):
    try:
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        if 'net_value' in df.columns:
            df = df.rename(columns={'date': '日期', 'net_value': '收盘'})
        if '日期' not in df.columns or '收盘' not in df.columns: return None

        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)
        if len(df) < 60: return None # 增加样本量以支持MA20
        
        if '成交额' in df.columns:
            if df['成交额'].iloc[-5:].mean() < LIQUIDITY_LIMIT: return None

        curr_p = df['收盘'].iloc[-1]
        prev_p = df['收盘'].iloc[-2]
        
        # 指标计算：双RSI + 双BIAS + 趋势线
        df['rsi6'] = calculate_rsi(df['收盘'], 6)
        df['rsi14'] = calculate_rsi(df['收盘'], 14)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['ma20'] = df['收盘'].rolling(window=20).mean()
        df['ma10'] = df['收盘'].rolling(window=10).mean() # 用于止盈趋势
        df['bias6'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
        df['bias20'] = ((df['收盘'] - df['ma20']) / df['ma20']) * 100
        df['max_high'] = df['收盘'].rolling(window=RETR_WINDOW).max()
        df['retr'] = ((df['收盘'] - df['max_high']) / df['max_high']) * 100
        
        df['in_watch'] = df['retr'] <= RETR_WATCH
        df['persist_days'] = df['in_watch'].groupby((df['in_watch'] != df['in_watch'].shift()).cumsum()).cumcount() + 1
        df.loc[~df['in_watch'], 'persist_days'] = 0

        curr = df.iloc[-1]
        code = os.path.splitext(os.path.basename(file_path))[0].zfill(6)
        name = NAME_MAP.get(code, "未知品种")
        
        if curr['in_watch']:
            score = 1
            divergence = check_strong_divergence(df)
            # RSI共振：短期超卖 + 中期不超买
            if curr['rsi6'] < 30 and curr['rsi14'] < 40: score += 2
            # BIAS共振：短期及中期均出现乖离
            if curr['bias6'] < -5 and curr['bias20'] < -7: score += 2
            # 强化背离加分
            if divergence: score += 2
            
            risk_level = "正常"
            if divergence: risk_level = "📈强力底背离"
            elif curr['rsi6'] > 50: risk_level = "🚩假摔陷阱"
                
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                '名称': name,
                '评分': score,
                '持续天数': int(curr['persist_days']),
                '风险预警': risk_level,
                '回撤%': round(curr['retr'], 2),
                'RSI6': round(curr['rsi6'], 2),
                'BIAS20': round(curr['bias20'], 2),
                'price': round(curr['收盘'], 4),
                'ma5_trend': "UP" if curr['ma6'] > df['ma6'].iloc[-2] else "DOWN"
            }
    except: return None

# ==========================================
# --- 5. 盈亏统计与趋势止盈逻辑 ---
# ==========================================
def get_performance_stats():
    history_files = glob.glob('202*/**/*.csv', recursive=True)
    perf_list = []
    for h_file in history_files:
        if 'perf' in h_file: continue
        try:
            h_df = pd.read_csv(h_file)
            for _, sig in h_df.iterrows():
                code = str(sig['fund_code']).zfill(6)
                raw_path = f'fund_data/{code}.csv'
                if not os.path.exists(raw_path): continue
                try: raw_df = pd.read_csv(raw_path, encoding='utf-8')
                except: raw_df = pd.read_csv(raw_path, encoding='gbk')
                if 'net_value' in raw_df.columns: raw_df = raw_df.rename(columns={'date': '日期', 'net_value': '收盘'})
                raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                
                idx_list = raw_df[raw_df['日期'] == str(sig['date'])].index
                if not idx_list.empty:
                    curr_idx = idx_list[0]
                    signal_price = sig['price']
                    after_signal_df = raw_df.iloc[curr_idx:].copy()
                    after_signal_df['ma5'] = after_signal_df['收盘'].rolling(window=5).mean()
                    after_signal_df['ma10'] = after_signal_df['收盘'].rolling(window=10).mean()
                    
                    latest = after_signal_df.iloc[-1]
                    max_profit = (after_signal_df['收盘'].max() - signal_price) / signal_price * 100
                    total_hold_change = (latest['收盘'] - signal_price) / signal_price * 100
                    
                    # 趋势判定：5日线死叉10日线
                    is_dead_cross = latest['ma5'] < latest['ma10'] and len(after_signal_df) > 5
                    
                    perf_list.append({
                        '日期': sig['date'], '代码': code, '名称': NAME_MAP.get(code, "未知"),
                        '评分': sig.get('评分', 1), '最新价': round(latest['收盘'], 4), 
                        '最高浮盈%': round(max_profit, 2), '总盈亏%': round(total_hold_change, 2),
                        '状态': "✅趋势向上" if not is_dead_cross else "🚨趋势走弱",
                        '死叉': "YES" if is_dead_cross else "NO"
                    })
        except: continue
    return pd.DataFrame(perf_list)

# ==========================================
# --- 6. 最终决策报告生成 ---
# ==========================================
def update_readme(current_res, perf_df):
    now_bj = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF/基金 策略雷达 (算法增强版)\n\n> 最后更新: `{now_bj}`\n\n"
    content += "### 🚩 战法铁律：\n- **双RSI共振**：RSI6 < 30 且 RSI14 < 40 确认为真底部。\n- **趋势止盈**：浮盈 > 5% 后，出现 **5日线死叉10日线** 或 **利润回吐3%** 强制离场。\n\n"
    
    if not perf_df.empty:
        active = perf_df.drop_duplicates(subset=['代码'])
        total_p = (active['总盈亏%'] / 100 * PORTFOLIO_UNIT).sum()
        avg_r = (total_p / (len(active)*PORTFOLIO_UNIT)) * 100 if len(active)>0 else 0
        content += f"## 💰 实战风控盘口\n> **总盈亏**: `¥{total_p:.2f} ({avg_r:+.2f}%)` | **风控**: `{'🛡️安全' if avg_r > STOP_BUY_LOSS_RATIO else '❌停买'}`\n\n"

    content += "## 🎯 实时信号 (双指标共振版)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        content += df[['date', 'fund_code', '名称', '评分', '风险预警', '回撤%', 'RSI6', 'BIAS20', 'price']].to_markdown(index=False) + "\n\n"

    content += "## 🔥 活跃买点 (趋势跟踪)\n"
    if not perf_df.empty:
        def decide_sell(row):
            if row['总盈亏%'] >= 5.0 and row['死叉'] == "YES": return "🚨 趋势反转，清仓！"
            if row['最高浮盈%'] > 8.0 and row['总盈亏%'] < (row['最高浮盈%'] - 3.0): return "🚨 利润回吐，结账"
            return row['状态']
        
        perf_df['操作建议'] = perf_df.apply(decide_sell, axis=1)
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
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
RETR_WATCH = -10.0         # 回撤阈值锁定为 -20.0
RETR_WINDOW = 250          # 250日实战周期
RSI_LOW = 30           
BIAS_LOW = -5.0        
LIQUIDITY_LIMIT = 10000000 # [风控] 日均成交额低于1000万的ETF不介入

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
# --- 3. 技术指标计算模块 ---
# ==========================================
def calculate_rsi(series, period=6):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs.fillna(0)))

def check_rsi_divergence(df, window=20):
    """RSI底背离检测"""
    if len(df) < window + 5: return False
    curr_price = df['收盘'].iloc[-1]
    curr_rsi = df['rsi'].iloc[-1]
    
    lookback_df = df.iloc[-(window+1):-1]
    if lookback_df.empty: return False
    
    min_price_idx = lookback_df['收盘'].idxmin()
    min_price_val = lookback_df['收盘'].min()
    min_price_rsi = lookback_df.loc[min_price_idx, 'rsi']
    
    if curr_price <= min_price_val and curr_rsi > min_price_rsi + 2:
        return True
    return False

# ==========================================
# --- 4. 单文件处理 (适配上传的CSV格式) ---
# ==========================================
def process_file(file_path):
    try:
        # 支持多种编码读取
        try: df = pd.read_csv(file_path, encoding='utf-8')
        except: df = pd.read_csv(file_path, encoding='gbk')
        
        # 字段兼容性清洗
        if '日期' not in df.columns or '收盘' not in df.columns:
            return None
            
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(by='日期').reset_index(drop=True)
        if len(df) < 30: return None

        # [新增] 流动性过滤：基于“成交额”字段
        if '成交额' in df.columns:
            recent_avg_amount = df['成交额'].iloc[-5:].mean()
            if recent_avg_amount < LIQUIDITY_LIMIT: return None

        # 数据异动监控
        curr_p = df['收盘'].iloc[-1]
        prev_p = df['收盘'].iloc[-2]
        # 拦截由于数据源错误导致的净值归1 (如510010早期数据为1.0左右，需结合均值判断)
        if curr_p == 1.0 and prev_p > 1.1: return None
        is_vol_alert = abs((curr_p - prev_p) / prev_p) > 0.09 

        # 指标计算
        df['rsi'] = calculate_rsi(df['收盘'], 6)
        df['ma6'] = df['收盘'].rolling(window=6).mean()
        df['bias'] = ((df['收盘'] - df['ma6']) / df['ma6']) * 100
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
            divergence = check_rsi_divergence(df)
            if curr['rsi'] < RSI_LOW: score += 2
            if curr['bias'] < BIAS_LOW: score += 2
            if divergence: score += 2
            
            risk_level = "正常"
            if is_vol_alert: risk_level = "⚠️净值异动"
            elif divergence: risk_level = "📈底背离形成"
            elif curr['rsi'] > 55 and score == 1: risk_level = "🚩高风险(陷阱)"
            elif score >= 5: risk_level = "🔥极高胜率(背离)"
            elif score >= 3: risk_level = "✅高胜率区"
                
            return {
                'date': str(curr['日期']).split(' ')[0],
                'fund_code': code,
                '名称': name,
                '评分': score,
                '持续天数': int(curr['persist_days']),
                '风险预警': risk_level,
                '回撤%': round(curr['retr'], 2),
                'RSI': round(curr['rsi'], 2),
                'BIAS': round(curr['bias'], 2),
                'price': round(curr['收盘'], 4)
            }
    except: return None

# ==========================================
# --- 5. 盈亏统计模块 (包含最高浮盈) ---
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
                
                raw_df['日期'] = pd.to_datetime(raw_df['日期']).dt.strftime('%Y-%m-%d')
                
                idx_list = raw_df[raw_df['日期'] == str(sig['date'])].index
                if not idx_list.empty:
                    curr_idx = idx_list[0]
                    signal_price = sig['price']
                    after_signal_df = raw_df.iloc[curr_idx:]
                    latest_price = after_signal_df['收盘'].iloc[-1]
                    
                    if latest_price == 1.0 and signal_price > 1.1: continue
                    
                    max_price_after = after_signal_df['收盘'].max()
                    max_profit = (max_price_after - signal_price) / signal_price * 100
                    
                    prev_price = raw_df.iloc[-2]['收盘'] if len(raw_df) > 1 else latest_price
                    daily_raw = (latest_price - prev_price) / prev_price * 100
                    color_tag = "🔴 " if daily_raw > 0 else "🟢 " if daily_raw < 0 else ""
                    
                    total_hold_change = (latest_price - signal_price) / signal_price * 100
                    
                    recovery_df = raw_df.iloc[curr_idx+1:]
                    back_days = "未回本"
                    back_idx = recovery_df[recovery_df['收盘'] >= signal_price].index
                    if not back_idx.empty: back_days = int(back_idx[0] - curr_idx)
                    
                    perf_list.append({
                        '日期': sig['date'], '代码': code, '名称': NAME_MAP.get(code, "未知"),
                        '评分': sig.get('评分', 1), '信号价': round(signal_price, 4), 
                        '最新价': round(latest_price, 4), '今日涨跌': f"{color_tag}{daily_raw:+.2f}%", 
                        '最高浮盈%': round(max_profit, 2), 
                        '总盈亏%': round(total_hold_change, 2), '回本天数': back_days,
                        '状态': "✅反弹中" if total_hold_change > 1 else "❌走弱" if total_hold_change < -3 else "⏳磨底中"
                    })
        except: continue
    return pd.DataFrame(perf_list)

# ==========================================
# --- 6. 报告生成 (README) ---
# ==========================================
def update_readme(current_res, perf_df):
    now_bj = (datetime.now(timezone.utc) + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
    content = f"# 🤖 ETF 策略雷达 (250日实战版)\n\n> 最后更新: `{now_bj}`\n\n"
    
    total_invested = 0
    total_profit_loss_val = 0
    avg_return_rate = 0
    is_budget_full = False
    is_panic_mode = False

    if not perf_df.empty:
        recent_limit = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        active_focus = perf_df[(perf_df['评分'] >= 3) & (perf_df['日期'] >= recent_limit)].drop_duplicates(subset=['代码'])
        
        if not active_focus.empty:
            total_invested = len(active_focus) * PORTFOLIO_UNIT
            total_profit_loss_val = (active_focus['总盈亏%'] / 100 * PORTFOLIO_UNIT).sum()
            avg_return_rate = (total_profit_loss_val / total_invested) * 100 if total_invested > 0 else 0
            is_budget_full = total_invested >= TOTAL_BUDGET_CAP
            is_panic_mode = avg_return_rate <= STOP_BUY_LOSS_RATIO

    content += "## 💰 实战风控控制台\n"
    content += f"> **模拟仓位**: `¥{total_invested} / ¥{TOTAL_BUDGET_CAP}` | **账户总盈亏**: `¥{total_profit_loss_val:.2f} ({avg_return_rate:+.2f}%)`\n"
    status_str = "🛡️ 状态良好"
    if is_budget_full: status_str = "⛔ 仓位已满"
    if is_panic_mode: status_str = "❌ 触发禁买令(亏损过大)"
    content += f"> **当前策略**: `{status_str}`\n\n"

    content += "## 🎯 抄底信号池 (-20%回撤 + 指标加分)\n"
    if current_res:
        df = pd.DataFrame(current_res).sort_values(['评分', '回撤%'], ascending=[False, True])
        def get_adv(r):
            if r['评分'] < 3: return "⏳ 观察中"
            if is_budget_full or is_panic_mode: return "❌ 停止建仓"
            return "✅ 分批建仓"
        df['操作建议'] = df.apply(get_adv, axis=1)
        content += df.to_markdown(index=False) + "\n\n"
    else:
        content += "> 💤 暂无超跌品种。\n\n"

    content += "## 🔥 信号后续追踪 (最高浮盈回盘)\n"
    if not perf_df.empty:
        track_df = perf_df.sort_values('日期', ascending=False).head(15)
        content += track_df.to_markdown(index=False) + "\n\n"

    with open('README.md', 'w', encoding='utf-8') as f: f.write(content)

# ==========================================
# --- 7. 主程序 ---
# ==========================================
def main():
    files = glob.glob('fund_data/*.csv')
    with Pool(cpu_count()) as p:
        results = [r for r in p.map(process_file, files) if r is not None]
    
    if results:
        folder = datetime.now().strftime('%Y/%m')
        os.makedirs(folder, exist_ok=True)
        pd.DataFrame(results).to_csv(f"{folder}/sig_{datetime.now().strftime('%d_%H%M%S')}.csv", index=False)
    
    perf_df = get_performance_stats()
    update_readme(results, perf_df)

if __name__ == "__main__":
    main()

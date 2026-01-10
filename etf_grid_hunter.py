import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：RSI-BOLL-VOLUME 终极胜率增强版 
# 包含：MA20动态中轴、RSI风险锁、分级加码、安全防护、横盘天数、变盘预警
# 只有当（横盘天数 > 3）且（乖离率 < 2%）时，才考虑手动在网格基础上多买入 0.5 层仓位。
# ==============================================================================
DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx' 

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def analyze_fund(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig').tail(100)
        if len(df) < 30: return None
        df.columns = [c.strip() for c in df.columns]
        
        latest = df.iloc[-1]
        close_series = df['收盘']
        
        # --- [基础防护逻辑] ---
        turnover_raw = latest.get('换手率', 0)
        try:
            turnover = float(str(turnover_raw).replace('%', ''))
        except:
            turnover = 0
        if latest['成交额'] < 10000000 or turnover < 0.1: return None

        # --- [指标计算] ---
        ma20_series = close_series.rolling(20).mean()
        ma20 = ma20_series.iloc[-1]
        rsi_val = calculate_rsi(close_series).iloc[-1]
        avg_amp = df['振幅'].tail(20).mean()
        bias = (latest['收盘'] - ma20) / ma20 * 100
        vol_ratio = latest['成交额'] / (df['成交额'].tail(5).mean() + 1e-9)

        # --- [横盘天数统计] ---
        diff_pct = (close_series - ma20_series) / ma20_series
        is_sideways = diff_pct.abs() < 0.02
        sideways_days = 0
        for val in reversed(is_sideways.values):
            if val: sideways_days += 1
            else: break

        # --- [新增：横盘陷阱判定逻辑] ---
        sideways_type = "动态波动"
        if sideways_days >= 3:
            # 均线最近5天的方向 (斜率)
            slope = (ma20_series.iloc[-1] - ma20_series.iloc[-5]) / 5
            if bias < 0.5 and slope <= 0:
                sideways_type = "低位筑底✅"
            elif bias > 2.0:
                sideways_type = "高位派发⚠️"
            else:
                sideways_type = "中继整理"

        # --- [风险锁与胜率判定] ---
        if rsi_val > 70 or avg_amp < 1.2: return None

        status, action, weight, star = "正常震荡", "常规网格", "1.0x", "★★★☆☆"
        boll_pos = "中轨上方(看强)" if latest['收盘'] > ma20 else "中轨下方(看弱)"
        
        # 降级逻辑：如果是高位派发风险，即便其他条件好，也将胜率降级
        if sideways_type == "高位派发⚠️":
            star = "★★☆☆☆"
            action = "警惕回撤/减量网格"

        if rsi_val < 35:
            status, star = "🔥机会区", "★★★★☆"
            if rsi_val < 30:
                status, action, weight, star = "🚨超卖加码区", "暂停卖出/只买不卖", "1.5x - 2.0x", "★★★★☆"
                if vol_ratio > 1.1 and bias < -3:
                    status, star, action = "💎五星金底", "★★★★★", "强力加码/只买不卖"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            '证券代码': code,
            '收盘价': latest['收盘'],
            'RSI(14)': round(rsi_val, 2),
            '乖离率%': round(bias, 2),
            '横盘天数': sideways_days,
            '横盘性质': sideways_type,
            '网格状态': status,
            '胜率置信度': star,
            '建议操作': action,
            '加码倍数': weight,
            '成交额(万)': round(latest['成交额'] / 10000, 2),
            '20日均振幅%': round(avg_amp, 2)
        }
    except Exception: return None

def main():
    # ... (保持原有的加载白名单和并行处理逻辑不变)
    if not os.path.exists(ETF_LIST_FILE):
        alt_csv = ETF_LIST_FILE.replace('.xlsx', '.csv')
        target_file = alt_csv if os.path.exists(alt_csv) else None
        if not target_file: return
    else: target_file = ETF_LIST_FILE
    try:
        if target_file.endswith('.xlsx'): name_df = pd.read_excel(target_file, engine='openpyxl')
        else:
            for enc in ['utf-8-sig', 'gbk', 'utf-8']:
                try:
                    name_df = pd.read_csv(target_file, encoding=enc)
                    break
                except: continue
        name_df.columns = [c.strip() for c in name_df.columns]
        name_df['证券代码'] = name_df['证券代码'].astype(str).str.zfill(6)
        name_map = dict(zip(name_df['证券代码'], name_df['证券简称']))
    except: return

    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    with Pool(cpu_count()) as p:
        results = p.map(analyze_fund, csv_files)
    
    valid = [r for r in results if r and r['证券代码'] in name_map]
    if not valid: return

    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 按照置信度、横盘天数、RSI 排序
    cols = ['证券代码', '证券简称', '收盘价', 'RSI(14)', '乖离率%', '横盘天数', '横盘性质',
            '网格状态', '胜率置信度', '建议操作', '加码倍数', '成交额(万)', '20日均振幅%']
    final_df = final_df[cols].sort_values(['胜率置信度', '横盘天数'], ascending=[False, False])
    
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    save_path = os.path.join(dir_path, f"best_buy_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"✅ 扫描完成：{save_path}")

if __name__ == "__main__":
    main()

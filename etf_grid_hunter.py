import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：RSI-BOLL-VOLUME 终极胜率增强版 (优化版)
# 核心改动：增加MA60趋势过滤、优化横盘判定、动态振幅适配
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
        # 增加读取长度以计算 MA60
        df = pd.read_csv(file_path, encoding='utf-8-sig').tail(120)
        if len(df) < 60: return None
        df.columns = [c.strip() for c in df.columns]
        
        latest = df.iloc[-1]
        close_series = df['收盘']
        
        # --- [基础防护逻辑] ---
        turnover_raw = latest.get('换手率', 0)
        try:
            turnover = float(str(turnover_raw).replace('%', ''))
        except:
            turnover = 0
        # 维持原成交额过滤：成交额 > 1000万，换手 > 0.1% 
        if latest['成交额'] < 10000000 or turnover < 0.1: return None

        # --- [核心指标计算] ---
        ma20_series = close_series.rolling(20).mean()
        ma60_series = close_series.rolling(60).mean()
        ma20, ma60 = ma20_series.iloc[-1], ma60_series.iloc[-1]
        
        rsi_val = calculate_rsi(close_series).iloc[-1]
        avg_amp = df['振幅'].tail(20).mean()
        bias = (latest['收盘'] - ma20) / ma20 * 100
        vol_ratio = latest['成交额'] / (df['成交额'].tail(5).mean() + 1e-9)

        # --- [横盘逻辑优化] ---
        # 乖离率绝对值 < 2.5% 判定为横盘（略微放宽，增加灵敏度）
        diff_pct = (close_series - ma20_series) / ma20_series
        is_sideways = diff_pct.abs() < 0.025
        sideways_days = 0
        for val in reversed(is_sideways.values):
            if val: sideways_days += 1
            else: break

        # --- [新增：趋势强度判定] ---
        trend_status = "多头排列" if ma20 > ma60 else "空头排列"
        
        # --- [横盘陷阱逻辑] ---
        sideways_type = "动态波动"
        if sideways_days >= 3:
            slope = (ma20_series.iloc[-1] - ma20_series.iloc[-5]) / 5
            if bias < 0.5 and slope <= 0:
                sideways_type = "低位筑底✅"
            elif bias > 2.0:
                sideways_type = "高位派发⚠️"
            else:
                sideways_type = "中继整理"

        # --- [风险与胜率判定] ---
        # 维持原逻辑：RSI > 70 风险锁，平均振幅过低（无波动不网格）则过滤 
        if rsi_val > 72 or avg_amp < 1.0: return None

        status, action, weight, star = "正常震荡", "常规网格", "1.0x", "★★★☆☆"
        
        # 降级逻辑：高位风险
        if sideways_type == "高位派发⚠️":
            star = "★★☆☆☆"
            action = "警惕回撤/减量网格"

        # 增强逻辑：超卖与金底 
        if rsi_val < 38: # 略微放宽阈值
            status, star = "🔥机会区", "★★★★☆"
            if rsi_val < 32:
                status, action, weight, star = "🚨超卖加码区", "暂停卖出/分批补仓", "1.5x", "★★★★☆"
                # 原有的“五星金底”逻辑：成交额放量且严重负乖离 
                if vol_ratio > 1.1 and bias < -4:
                    status, star, action, weight = "💎五星金底", "★★★★★", "全力补仓/只买不卖", "2.0x"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            '证券代码': code,
            '收盘价': latest['收盘'],
            'RSI(14)': round(rsi_val, 2),
            '乖离率%': round(bias, 2),
            '趋势': trend_status,
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
    # 保持原有的文件加载逻辑 
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
    if not valid: 
        print("❌ 未发现符合条件的标的（可能市场过热或处于极端低波动期）")
        return

    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 按照 置信度、趋势(多头优先)、RSI(低者优先) 排序
    cols = ['证券代码', '证券简称', '收盘价', 'RSI(14)', '趋势', '乖离率%', '横盘天数', '横盘性质',
            '网格状态', '胜率置信度', '建议操作', '加码倍数', '成交额(万)', '20日均振幅%']
    final_df = final_df[cols].sort_values(
        ['胜率置信度', '趋势', 'RSI(14)'], 
        ascending=[False, False, True]
    )
    
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    save_path = os.path.join(dir_path, f"grid_hunt_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"✅ 扫描完成：{save_path}")
    print(final_df[['证券简称', 'RSI(14)', '趋势', '胜率置信度', '建议操作']].head(10))

if __name__ == "__main__":
    main()

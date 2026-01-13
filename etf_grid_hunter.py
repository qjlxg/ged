import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：Alpha Hunter V3 能量趋势网格全能版 (最终完整版)
# 包含功能：
# 1. [核心逻辑] MA20动态中轴、RSI风险锁、分级加码、5星金底判定
# 2. [量价背离] 检测缩量上涨（诱多）与放量下跌（恐慌）
# 3. [动态适配] 基于 ATR 自动调节不同 ETF 的横盘判定阈值
# 4. [趋势过滤] MA60 趋势生命线，区分多头排列与空头排列
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
        # 读取 120 天数据确保 MA60 和 ATR 计算准确
        df = pd.read_csv(file_path, encoding='utf-8-sig').tail(120)
        if len(df) < 60: return None
        df.columns = [c.strip() for c in df.columns]
        
        latest = df.iloc[-1]
        close_series = df['收盘']
        vol_series = df['成交额']
        
        # --- [1. 基础过滤逻辑] ---
        turnover_raw = latest.get('换手率', 0)
        try:
            turnover = float(str(turnover_raw).replace('%', ''))
        except:
            turnover = 0
        if latest['成交额'] < 10000000 or turnover < 0.1: return None

        # --- [2. 关键指标计算] ---
        ma20_s = close_series.rolling(20).mean()
        ma60_s = close_series.rolling(60).mean()
        ma20, ma60 = ma20_s.iloc[-1], ma60_s.iloc[-1]
        
        bias = (latest['收盘'] - ma20) / ma20 * 100
        rsi_val = calculate_rsi(close_series).iloc[-1]
        
        # ATR 动态波动率适配
        high_low = df['最高'] - df['最低']
        atr = high_low.rolling(14).mean().iloc[-1]
        relative_atr = (atr / latest['收盘']) * 100
        
        # 量能系统 (5日量比20日量)
        vol_ma5 = vol_series.tail(5).mean()
        vol_ma20 = vol_series.tail(20).mean()
        vol_ratio = vol_ma5 / (vol_ma20 + 1e-9)

        # --- [3. 动态横盘判定逻辑] ---
        # 自动根据波动率调整阈值：波动大的宽，波动小的窄
        dynamic_threshold = max(0.018, relative_atr * 0.5 / 100)
        is_sideways = ((close_series - ma20_s) / ma20_s).abs() < dynamic_threshold
        sideways_days = 0
        for val in reversed(is_sideways.values):
            if val: sideways_days += 1
            else: break

        # --- [4. 状态综合判定系统] ---
        trend_status = "多头排列" if ma20 > ma60 else "空头排列"
        
        # 横盘性质判定 (基于 MA20 斜率)
        sideways_type = "动态波动"
        if sideways_days >= 3:
            slope = (ma20_s.iloc[-1] - ma20_s.iloc[-5]) / 5
            if bias < 0.5 and slope <= 0: sideways_type = "低位筑底✅"
            elif bias > 2.0: sideways_type = "高位派发⚠️"
            else: sideways_type = "中继整理"

        # 量价背离侦测 (上涨但没量 = 诱多)
        is_divergence = (latest['收盘'] > ma20) and (vol_ratio < 0.8)

        # 默认初始状态
        status, action, weight, star = "正常震荡", "常规网格", "1.0x", "★★★☆☆"

        # A. 抄底判定 (金底逻辑)
        if rsi_val < 38:
            status, star, action = "🔥机会区", "★★★★☆", "分批补仓"
            if rsi_val < 32:
                status, action, weight = "🚨超卖加码", "暂停卖出/积极加码", "1.5x"
                # 黄金坑判定：严重负乖离 + 恐慌放量
                if vol_ratio > 1.15 and bias < -4.5:
                    status, star, action, weight = "💎五星金底", "★★★★★", "全力买入/持有", "2.0x"
        
        # B. 风险与背离判定
        elif rsi_val > 70 or is_divergence:
            star = "★★☆☆☆"
            if is_divergence:
                status, action = "🚫缩量诱多", "停止买入/仅卖不买"
            else:
                status, action = "⚠️高位超买", "网格收缩/止盈"
        
        # C. 趋势突破判定
        elif trend_status == "多头排列" and 0 < bias < 2.5 and sideways_days >= 4:
            status, star, action = "🚀蓄势突破", "★★★★☆", "持仓待涨/网格上移"

        # 振幅过滤：过滤掉死鱼标的
        avg_amp = df['振幅'].tail(20).mean()
        if avg_amp < 1.0: return None 

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            '证券代码': code,
            '收盘价': latest['收盘'],
            'RSI(14)': round(rsi_val, 2),
            '趋势': trend_status,
            '乖离率%': round(bias, 2),
            '量能比': round(vol_ratio, 2),
            '波动率%': round(relative_atr, 2),
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
    # 查找列表文件
    target_file = None
    for f in [ETF_LIST_FILE, ETF_LIST_FILE.replace('.xlsx', '.csv')]:
        if os.path.exists(f):
            target_file = f
            break
    if not target_file: return

    # 加载映射
    try:
        if target_file.endswith('.xlsx'):
            name_df = pd.read_excel(target_file, engine='openpyxl')
        else:
            name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df.columns = [c.strip() for c in name_df.columns]
        name_df['证券代码'] = name_df['证券代码'].astype(str).str.zfill(6)
        name_map = dict(zip(name_df['证券代码'], name_df['证券简称']))
    except: return

    # 并行处理
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🔍 启动 Alpha Hunter V3... 深度分析 {len(csv_files)} 个品种...")
    with Pool(cpu_count()) as p:
        results = p.map(analyze_fund, csv_files)
    
    valid = [r for r in results if r and r['证券代码'] in name_map]
    if not valid:
        print("💡 当前无符合高置信度信号的品种。")
        return

    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 按照置信度降序、RSI 升序排列
    cols = ['证券代码', '证券简称', '收盘价', 'RSI(14)', '趋势', '乖离率%', '量能比', '波动率%', 
            '横盘天数', '横盘性质', '网格状态', '胜率置信度', '建议操作', '加码倍数', '成交额(万)', '20日均振幅%']
    final_df = final_df[cols].sort_values(by=['胜率置信度', 'RSI(14)'], ascending=[False, True])
    
    # 保存结果
    now = datetime.now()
    os.makedirs(now.strftime('%Y/%m'), exist_ok=True)
    save_path = os.path.join(now.strftime('%Y/%m'), f"alpha_hunter_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 扫描成功！数据保存在：{save_path}")
    print("-" * 80)
    # 修正了字段名，确保不再报 KeyError
    print(final_df[['证券简称', '网格状态', '胜率置信度', '建议操作', '趋势', '量能比']].head(10))

if __name__ == "__main__":
    main()

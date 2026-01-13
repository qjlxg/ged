import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：Alpha Hunter V3 能量趋势网格全能版
# 整合功能：
# 1. [经典网格] MA20乖离 + RSI超卖 + 5星金底判定
# 2. [趋势防护] MA60生命线过滤，区分多头/空头市场
# 3. [量价侦测] 5日/20日量能比，识别缩量诱多与放量杀跌
# 4. [动态适配] ATR自适应波动率，自动调整横盘判定标准
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
        
        # --- [1. 流动性与基础过滤] ---
        turnover_raw = latest.get('换手率', 0)
        try:
            turnover = float(str(turnover_raw).replace('%', ''))
        except:
            turnover = 0
        if latest['成交额'] < 10000000 or turnover < 0.1: return None

        # --- [2. 计算核心技术指标] ---
        # 均线系统
        ma20_s = close_series.rolling(20).mean()
        ma60_s = close_series.rolling(60).mean()
        ma20, ma60 = ma20_s.iloc[-1], ma60_s.iloc[-1]
        
        # 乖离率与 RSI
        bias = (latest['收盘'] - ma20) / ma20 * 100
        rsi_val = calculate_rsi(close_series).iloc[-1]
        
        # ATR 动态波动率 (用于自适应横盘判定)
        high_low = df['最高'] - df['最低']
        atr = high_low.rolling(14).mean().iloc[-1]
        relative_atr = (atr / latest['收盘']) * 100
        
        # 量能系统
        vol_ma5 = vol_series.tail(5).mean()
        vol_ma20 = vol_series.tail(20).mean()
        vol_ratio = vol_ma5 / (vol_ma20 + 1e-9)

        # --- [3. 横盘逻辑 - 动态阈值版] ---
        # 波动率大的品种阈值高，小的则低
        dynamic_threshold = max(0.018, relative_atr * 0.5 / 100)
        is_sideways = ((close_series - ma20_s) / ma20_s).abs() < dynamic_threshold
        sideways_days = 0
        for val in reversed(is_sideways.values):
            if val: sideways_days += 1
            else: break

        # --- [4. 状态综合判定系统] ---
        trend_status = "多头排列" if ma20 > ma60 else "空头排列"
        
        # 横盘性质判定
        sideways_type = "动态波动"
        if sideways_days >= 3:
            slope = (ma20_s.iloc[-1] - ma20_s.iloc[-5]) / 5
            if bias < 0.5 and slope <= 0: sideways_type = "低位筑底✅"
            elif bias > 2.0: sideways_type = "高位派发⚠️"
            else: sideways_type = "中继整理"

        # 核心逻辑：量价背离检测
        is_divergence = (latest['收盘'] > ma20) and (vol_ratio < 0.85)

        # 默认网格状态
        status, action, weight, star = "正常震荡", "常规网格", "1.0x", "★★★☆☆"

        # 逻辑 A：金底与超卖判定 (买点)
        if rsi_val < 38:
            status, star = "🔥机会区", "★★★★☆"
            action = "分批补仓"
            if rsi_val < 32:
                status, action, weight = "🚨超卖加码", "暂停卖出/积极买入", "1.5x"
                # 终极金底：严重负乖离 + 放量
                if vol_ratio > 1.15 and bias < -4.5:
                    status, star, action, weight = "💎五星金底", "★★★★★", "全力买入", "2.0x"
        
        # 逻辑 B：高位与背离风险 (卖点/防守)
        elif rsi_val > 70 or is_divergence:
            if is_divergence:
                status, star, action = "🚫缩量诱多", "★★☆☆☆", "停止买入/仅卖出"
            else:
                status, star, action = "⚠️高位超买", "★★☆☆☆", "网格减量/止盈"
        
        # 逻辑 C：多头突破判定
        elif trend_status == "多头排列" and 0 < bias < 2.5 and sideways_days >= 4:
            status, star, action = "🚀蓄势突破", "★★★★☆", "持仓待涨/网格上移"

        # --- [5. 过滤器：过滤掉无波动的死鱼] ---
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
    # 查找 ETF 列表文件
    target_file = None
    for f in [ETF_LIST_FILE, ETF_LIST_FILE.replace('.xlsx', '.csv')]:
        if os.path.exists(f):
            target_file = f
            break
    if not target_file:
        print("❌ 找不到 ETF 列表文件")
        return

    # 加载名称映射
    try:
        if target_file.endswith('.xlsx'):
            name_df = pd.read_excel(target_file, engine='openpyxl')
        else:
            name_df = pd.read_csv(target_file, encoding='utf-8-sig')
        name_df.columns = [c.strip() for c in name_df.columns]
        name_df['证券代码'] = name_df['证券代码'].astype(str).str.zfill(6)
        name_map = dict(zip(name_df['证券代码'], name_df['证券简称']))
    except Exception as e:
        print(f"❌ 列表读取失败: {e}")
        return

    # 并行分析
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🔍 正在深度扫描 {len(csv_files)} 个品种...")
    with Pool(cpu_count()) as p:
        results = p.map(analyze_fund, csv_files)
    
    valid = [r for r in results if r and r['证券代码'] in name_map]
    if not valid:
        print("💡 当前市场未匹配到高胜率信号，建议空仓等待。")
        return

    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 字段顺序排列
    cols = ['证券代码', '证券简称', '收盘价', 'RSI(14)', '趋势', '乖离率%', '量能比', '波动率%', 
            '横盘天数', '横盘性质', '网格状态', '胜率置信度', '建议操作', '加码倍数', '成交额(万)', '20日均振幅%']
    
    # 核心排序：优先展示高置信度标的
    final_df = final_df[cols].sort_values(
        by=['胜率置信度', 'RSI(14)'], 
        ascending=[False, True]
    )
    
    # 保存结果
    now = datetime.now()
    os.makedirs(now.strftime('%Y/%m'), exist_ok=True)
    save_path = os.path.join(now.strftime('%Y/%m'), f"alpha_hunter_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 扫描完成！报告已生成：{save_path}")
    print("-" * 50)
    print(final_df[['证券简称', '网格状态', '置信度', '操作指令', '量能比']].head(10))

if __name__ == "__main__":
    main()

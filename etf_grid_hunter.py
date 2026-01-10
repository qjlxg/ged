import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：RSI-BOLL 进阶安全网格战法 (完全体)
# ==============================================================================
# 【1. 核心哲学】：
#    本战法属于“震荡偏强”型策略。利用 BOLL 判断大趋势，利用 RSI 捕捉极值机会。
# 【2. 逻辑拆解】：
#    - 动态中轴 (MA20)：价格在中轴上方视为强震荡，下方视为弱震荡。网格随中轴漂移。
#    - 风险锁 (RSI > 70)：高位钝化风险极高。逻辑：【只出不进】，脚本直接剔除此类标的。
#    - 机会区 (RSI < 30)：超卖区。逻辑：【只进不出】，触发“分级加码”指令。
#    - 安全屏障：
#        - 流动性：日成交额 < 1000万的标的不碰（防流动性陷阱）。
#        - 活跃度：20日均振幅 < 1.2% 的标的不碰（无波动，网格无法套利）。
#        - 白名单：仅交易 ETF 列表内的标的（防个股暴雷/清盘）。
# ==============================================================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx'

def calculate_rsi(series, period=14):
    """
    计算指数平滑 RSI (Wilder's RSI)
    逻辑：反映一段时间内上涨压力与下跌压力的比值。
    """
    delta = series.diff()
    # 分别计算上涨幅度和下跌幅度
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def analyze_fund(file_path):
    """
    单基金扫描逻辑：实现战法核心过滤
    """
    try:
        # 优化：仅读取最近 100 行（约 5 个月数据），足以计算 MA20 和 RSI
        df = pd.read_csv(file_path, encoding='utf-8-sig').tail(100)
        if len(df) < 30: return None
        
        df.columns = [c.strip() for c in df.columns]
        latest = df.iloc[-1]
        
        # --- [战法逻辑：流动性风控] ---
        # 目的：确保买得进、卖得掉。日成交额低于 1000 万的 ETF 容易出现买卖价差过大。
        if latest['成交额'] < 10000000:
            return None

        close_series = df['收盘']
        
        # --- [战法逻辑：BOLL 中轴动态定位] ---
        # 目的：判断当前标的在 20 日均线的位置。
        ma20 = close_series.rolling(20).mean().iloc[-1]
        boll_pos = "中轨上方(看强)" if latest['收盘'] > ma20 else "中轨下方(看弱)"
        
        # --- [战法逻辑：RSI 风险锁/机会窗] ---
        # 目的：利用 RSI 指标进行逆向操作。
        rsi_all = calculate_rsi(close_series)
        rsi_val = rsi_all.iloc[-1]
        
        # --- [战法逻辑：振幅空间] ---
        # 目的：网格交易靠波动赚钱，若近期平均振幅过小，手续费会吃掉大部分利润。
        avg_amp = df['振幅'].tail(20).mean()
        
        # --- [核心逻辑：剔除策略] ---
        # 1. 剔除超买区（RSI > 70）：此时处于风险区，不在我们的“购买清单”内。
        # 2. 剔除死鱼股（振幅 < 1.2%）：这种标的做网格是浪费资金效率。
        if rsi_val > 70 or avg_amp < 1.2:
            return None
            
        # --- [状态判定：网格动作指引] ---
        status = "正常震荡"
        action = "常规网格"
        weight = "1.0x"
        
        # 触发超卖加码逻辑 (马丁变种)
        if rsi_val < 30:
            status = "🔥超卖/机会区"
            action = "暂停卖出/执行买入"
            weight = "1.5x - 2.0x (加码)"
            
        code = os.path.basename(file_path).replace('.csv', '')
        return {
            '证券代码': code,
            '收盘价': latest['收盘'],
            '成交额(万)': round(latest['成交额'] / 10000, 2),
            'RSI(14)': round(rsi_val, 2),
            '网格状态': status,
            '布林位置': boll_pos,
            '建议操作': action,
            '分级加码倍数': weight,
            '20日均振幅%': round(avg_amp, 2),
            '中轨(MA20)': round(ma20, 3)
        }
    except Exception:
        return None

def main():
    # --- [白名单加载] ---
    if not os.path.exists(ETF_LIST_FILE):
        print(f"找不到 {ETF_LIST_FILE}，请确保白名单文件存在。")
        return
        
    # 自动识别 Excel 或 CSV 格式的白名单
    if ETF_LIST_FILE.endswith('.xlsx'):
        name_df = pd.read_excel(ETF_LIST_FILE)
    else:
        name_df = pd.read_csv(ETF_LIST_FILE, encoding='utf-8-sig')
        
    # 格式化代码列，确保 00519 这种代码不会变成 519
    name_df['证券代码'] = name_df['证券代码'].astype(str).str.zfill(6)
    name_map = dict(zip(name_df['证券代码'], name_df['证券简称']))

    # --- [并行扫描] ---
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"🚀 开始扫描 {len(csv_files)} 个标的，基于 RSI-BOLL 完全体战法逻辑...")
    
    with Pool(cpu_count()) as p:
        results = p.map(analyze_fund, csv_files)
    
    # --- [二次过滤：白名单比对] ---
    valid = [r for r in results if r and r['证券代码'] in name_map]
    
    if not valid:
        print("💡 结论：今日市场无符合【安全网格买入】条件的标的。")
        return

    # --- [结果整理与排序] ---
    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 按照 RSI 从低到高排序，RSI 越低意味着安全边际相对越高
    cols = ['证券代码', '证券简称', '收盘价', '成交额(万)', 'RSI(14)', '网格状态', 
            '布林位置', '建议操作', '分级加码倍数', '20日均振幅%']
    final_df = final_df[cols].sort_values('RSI(14)')
    
    # --- [存储输出] ---
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    save_path = os.path.join(dir_path, f"fund_to_buy_{now.strftime('%Y%m%d')}.csv")
    
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    
    print("-" * 50)
    print(f"✅ 分析完成！建议购买清单已导出至: {save_path}")
    print(f"🔥 当前超卖机会标的数: {len(final_df[final_df['RSI(14)'] < 30])}")
    print("-" * 50)

if __name__ == "__main__":
    main()

import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：RSI-BOLL-VOLUME 终极胜率增强版 (3万元实盘专用)
# ==============================================================================
# 【保持完整核心功能】：
# 1. 动态中轴 (MA20)：价格在中轴上方视为强震，下方为弱震。
# 2. RSI 风险锁：RSI > 70 强制剔除(只卖不买)；RSI < 30 进入机会区(只买不卖)。
# 3. 分级加码：RSI < 30 触发马丁变种 1.5x - 2.0x 加码建议。
# 4. 安全防护：日成交额 > 1000万 且 必须在“ETF列表”白名单内。
# 5. 筛选标准：20日平均振幅 > 1.2% 确保套利空间。
#
# 【新增胜率增强因子】：
# 1. 量价协同：RSI < 35 且 今日成交额 > 5日均额(量比>1)，识别“金底”信号。
# 2. 换手率过滤：剔除换手率 < 0.1% 的僵尸品种，确保秒级成交。
# 3. 乖离率检查：计算现价偏离 MA20 的比例，辅助判定是否跌透。
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
        # 读取最近 100 行，确保指标计算准确
        df = pd.read_csv(file_path, encoding='utf-8-sig').tail(100)
        if len(df) < 30: return None
        df.columns = [c.strip() for c in df.columns]
        
        latest = df.iloc[-1]
        close_series = df['收盘']
        
        # --- [逻辑 4 & 新增因子 2：安全与换手率防护] ---
        # 必须满足：成交额 > 1000万 且 换手率 > 0.1%
        turnover = latest.get('换手率', 0)
        if latest['成交额'] < 10000000 or turnover < 0.1:
            return None

        # --- [技术指标计算] ---
        ma20 = close_series.rolling(20).mean().iloc[-1]
        rsi_val = calculate_rsi(close_series).iloc[-1]
        avg_amp = df['振幅'].tail(20).mean()
        
        # 新增因子 3：乖离率 (Bias) = (现价 - MA20) / MA20
        bias = (latest['收盘'] - ma20) / ma20 * 100
        
        # 新增因子 1：量比 (今日成交额 / 5日均额)
        vol_ratio = latest['成交额'] / (df['成交额'].tail(5).mean() + 1e-9)

        # --- [逻辑 2 & 逻辑 5：核心剔除标准] ---
        # 剔除 RSI > 70 (超买只卖不买) 和 振幅 < 1.2% (无套利空间)
        if rsi_val > 70 or avg_amp < 1.2:
            return None

        # --- [逻辑 1 & 3：状态与动作判定] ---
        status = "正常震荡"
        action = "常规网格"
        weight = "1.0x"
        star = "★★★☆☆" # 基础胜率
        
        # 判定布林位置 (逻辑 1)
        boll_pos = "中轨上方(看强)" if latest['收盘'] > ma20 else "中轨下方(看弱)"
        
        # 触发机会区 (逻辑 2 & 3)
        if rsi_val < 35:
            status = "🔥机会区"
            if rsi_val < 30:
                status = "🚨超卖加码区"
                action = "暂停卖出/只买不卖"
                weight = "1.5x - 2.0x"
                star = "★★★★☆"
                
                # [胜率增强：量价协同因子]
                if vol_ratio > 1.1 and bias < -3:
                    status = "💎五星金底"
                    star = "★★★★★"
                    action = "强力加码/只买不卖"

        code = os.path.basename(file_path).replace('.csv', '')
        return {
            '证券代码': code,
            '收盘价': latest['收盘'],
            '成交额(万)': round(latest['成交额'] / 10000, 2),
            '换手率%': round(turnover, 2),
            '量比': round(vol_ratio, 2),
            'RSI(14)': round(rsi_val, 2),
            '乖离率%': round(bias, 2),
            '网格状态': status,
            '胜率置信度': star,
            '布林位置': boll_pos,
            '建议操作': action,
            '加码倍数': weight,
            '20日均振幅%': round(avg_amp, 2),
            '中轨(MA20)': round(ma20, 3)
        }
    except Exception:
        return None

def main():
    # 白名单加载 (逻辑 4)
    if not os.path.exists(ETF_LIST_FILE):
        print(f"缺失白名单文件: {ETF_LIST_FILE}")
        return
    name_df = pd.read_csv(ETF_LIST_FILE, encoding='utf-8-sig')
    name_df['证券代码'] = name_df['证券代码'].astype(str).str.zfill(6)
    name_map = dict(zip(name_df['证券代码'], name_df['证券简称']))

    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"📡 正在扫描 {len(csv_files)} 个标的...")
    
    with Pool(cpu_count()) as p:
        results = p.map(analyze_fund, csv_files)
    
    valid = [r for r in results if r and r['证券代码'] in name_map]
    
    if not valid:
        print("💡 今日市场未扫描到符合买入逻辑的优质标的。")
        return

    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 按照胜率和RSI排序，最值得买的排在最前面
    cols = ['证券代码', '证券简称', '收盘价', '成交额(万)', 'RSI(14)', '量比', '乖离率%', 
            '网格状态', '胜率置信度', '布林位置', '建议操作', '加码倍数', '20日均振幅%']
    final_df = final_df[cols].sort_values(['胜率置信度', 'RSI(14)'], ascending=[False, True])
    
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    save_path = os.path.join(dir_path, f"best_buy_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    
    print("-" * 30)
    print(f"✅ 筛选成功！购买清单已更新: {save_path}")
    print(f"🚀 五星推荐标的数: {len(final_df[final_df['胜率置信度'] == '★★★★★'])}")
    print("-" * 30)

if __name__ == "__main__":
    main()

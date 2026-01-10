import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：RSI-BOLL 进阶安全网格战法 (完全体)
# 
# 【买卖逻辑 - 已找回并强化】：
# 1. 动态中轴：利用 BOLL 中轨（20日线）判断强弱。价格在中轨上，网格区间随之上移。
# 2. RSI 风险锁：
#    - RSI > 70（超买）：价格进入风险区，脚本自动剔除（只卖不买逻辑）。
#    - RSI < 30（超卖）：进入机会区，状态显示“🔥超卖”，执行“只买不卖”。
# 3. 分级加码（马丁变种）：在超卖区（RSI < 30）给出 1.5x - 2.0x 加码建议。
# 4. 安全防护：日成交额必须 > 1000万，且必须在“ETF列表”白名单内，防止清盘。
# 5. 筛选标准：20日平均振幅 > 1.2%（套利空间）。
# ==========================================

DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx'

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def analyze_fund(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        if len(df) < 30: return None
        df.columns = [c.strip() for c in df.columns]
        
        latest = df.iloc[-1]
        
        # --- 1. 流动性初筛 (1000万日成交额) ---
        if latest['成交额'] < 10000000:
            return None

        close_series = df['收盘']
        # --- 2. BOLL 中轴计算 ---
        ma20 = close_series.rolling(20).mean().iloc[-1]
        
        # --- 3. RSI 风险锁计算 ---
        rsi_all = calculate_rsi(close_series)
        rsi_val = rsi_all.iloc[-1]
        
        # --- 4. 振幅筛选 ---
        avg_amp = df['振幅'].tail(20).mean()
        
        # --- 5. 战法过滤与逻辑判定 ---
        # 只要 RSI > 70 (超买) 就剔除，因为此时“只卖不买”，不在购买清单内
        if rsi_val > 70 or avg_amp < 1.2:
            return None
            
        status = "正常震荡"
        action = "常规网格"
        weight = "1.0x"
        
        if rsi_val < 30:
            status = "🔥超卖/机会区"
            action = "暂停卖出/执行买入"
            weight = "1.5x - 2.0x (加码)"
            
        boll_pos = "中轨上方(看强)" if latest['收盘'] > ma20 else "中轨下方(看弱)"

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
    except:
        return None

def main():
    # 检查白名单
    if not os.path.exists(ETF_LIST_FILE):
        print(f"找不到 {ETF_LIST_FILE}")
        return
    name_df = pd.read_csv(ETF_LIST_FILE)
    name_map = dict(zip(name_df['证券代码'].astype(str).str.zfill(6), name_df['证券简称']))

    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"正在分析 {len(csv_files)} 个基金数据...")
    
    with Pool(cpu_count()) as p:
        results = p.map(analyze_fund, csv_files)
    
    # 过滤出符合战法的且在白名单内的标的
    valid = [r for r in results if r and r['证券代码'] in name_map]
    
    if not valid:
        print("今日无可购买的符合战法逻辑的标的。")
        return

    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 排序列名并输出
    cols = ['证券代码', '证券简称', '收盘价', '成交额(万)', 'RSI(14)', '网格状态', '布林位置', '建议操作', '分级加码倍数', '20日均振幅%']
    final_df = final_df[cols].sort_values('RSI(14)')
    
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    save_path = os.path.join(dir_path, f"fund_to_buy_{now.strftime('%Y%m%d')}.csv")
    
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"战法分析完成！购买建议已存入: {save_path}")

if __name__ == "__main__":
    main()

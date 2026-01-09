import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：RSI-BOLL 动态增强网格战法
# 
# 【买卖逻辑说明】：
# 1. 动态中轴：利用 BOLL 中轨（20日线）判断强弱。价格在中轨上，网格区间随之上移。
# 2. RSI 风险锁：
#    - RSI > 70（超买）：价格进入风险区，网格“只卖不买”，防止高位满仓。
#    - RSI < 30（超卖）：价格进入机会区，网格“只买不卖”，防止低位踏空。
# 3. 分级加码（马丁变种）：在超卖区（RSI < 30）建议加大买入权重至 1.5x - 2.0x，摊薄成本。
# 4. 筛选标准：20日平均振幅 > 1.2%（确保网格有足够的套利空间）。
# ==========================================

# 目录已按要求修改为 fund_data
DATA_DIR = 'fund_data'
ETF_LIST_FILE = 'ETF列表.xlsx - Sheet1.csv'

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def analyze_fund(file_path):
    """分析单个 ETF 基金数据"""
    try:
        # 使用 utf-8-sig 处理可能存在的 BOM
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        if len(df) < 30: return None
        
        # 清理列名空格
        df.columns = [c.strip() for c in df.columns]
        latest = df.iloc[-1]
        close_series = df['收盘']
        
        # 1. 计算 BOLL 指标
        ma20 = close_series.rolling(20).mean()
        std20 = close_series.rolling(20).std()
        curr_ma = ma20.iloc[-1]
        
        # 2. 计算 RSI 指标
        rsi_val = calculate_rsi(close_series).iloc[-1]
        
        # 3. 活跃度：20日平均振幅
        avg_amp = df['振幅'].tail(20).mean()
        
        # --- 战法决策逻辑 ---
        status = "正常震荡"
        action = "常规网格"
        weight = "1.0x"
        
        if rsi_val > 70:
            status = "⚠️超买区"
            action = "暂停买入/逢高减仓"
        elif rsi_val < 30:
            status = "🔥超卖区"
            action = "暂停卖出/分级加码买入"
            weight = "1.5x - 2.0x"
            
        boll_pos = "中轨上方(看强)" if latest['收盘'] > curr_ma else "中轨下方(看弱)"

        # 筛选条件：振幅需具备基本套利价值
        if avg_amp > 1.0:
            code = os.path.basename(file_path).replace('.csv', '')
            return {
                '证券代码': code,
                '收盘价': latest['收盘'],
                'RSI(14)': round(rsi_val, 2),
                '网格状态': status,
                '布林位置': boll_pos,
                '操作建议': action,
                '加码倍数': weight,
                '20日均振幅%': round(avg_amp, 2)
            }
    except Exception:
        return None

def main():
    # 路径存在性检查
    if not os.path.exists(DATA_DIR):
        print(f"错误: 目录 {DATA_DIR} 不存在！请检查仓库中文件夹名称。")
        return

    # 1. 加载映射文件
    name_map = {}
    if os.path.exists(ETF_LIST_FILE):
        name_df = pd.read_csv(ETF_LIST_FILE)
        # 适配证券代码格式
        name_map = dict(zip(name_df['证券代码'].astype(str).str.zfill(6), name_df['证券简称']))

    # 2. 并行处理
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"[{datetime.now()}] 正在扫描 {DATA_DIR} 下的 {len(csv_files)} 个文件...")
    
    if not csv_files:
        print(f"警告: {DATA_DIR} 目录下未找到任何 .csv 文件。")
        return

    with Pool(cpu_count()) as p:
        results = p.map(analyze_fund, csv_files)
    
    valid_results = [r for r in results if r is not None]
    
    if not valid_results:
        print("未发现满足波动率条件的基金标的。")
        return

    # 3. 整理输出
    final_df = pd.DataFrame(valid_results)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map.get(x, '未知'))
    
    # 按振幅降序，找到最适合网格的标的
    final_df = final_df.sort_values('20日均振幅%', ascending=False)
    
    # 4. 保存结果
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    
    file_name = f"etf_grid_hunter_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_path, file_name)
    
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"分析成功，结果存入: {save_path}")

if __name__ == "__main__":
    main()

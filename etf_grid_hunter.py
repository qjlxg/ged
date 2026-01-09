import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：RSI-BOLL 动态增强网格战法
# 
# 【操作要领与买卖逻辑】：
# 1. 中轴线判定：利用布林带中轨(BOLL Mid)作为网格中心。若现价高于中轨，代表处于强势区，应上移网格区间。
# 2. RSI 风险控制：
#    - RSI > 70（超买）：价格可能回调，网格策略应“暂停买入单”，只执行卖出单减仓。
#    - RSI < 30（超卖）：价格严重低估，网格策略应“暂停卖出单”，防止被震仓出局。
# 3. 分级加码逻辑：在 RSI < 30 时，建议开启马丁格尔倍投。例如下跌 1% 后，买入量由 1 份增加到 1.5 份，有效摊薄成本。
# 4. 筛选逻辑：排除流动性极差、震荡极小的品种，寻找“活”的品种进行网格。
# ==========================================

DATA_DIR = 'stock_data'
ETF_LIST_FILE = 'ETF列表.xlsx - Sheet1.csv'

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    # 避免除以零
    rs = gain / (loss + 1e-9)
    return 100 - (100 / (1 + rs))

def analyze_etf(file_path):
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        # 统一清理列名
        df.columns = [c.strip() for c in df.columns]
        
        # 关键技术指标计算
        close_series = df['收盘']
        latest = df.iloc[-1]
        
        # 1. 布林线 (BOLL)
        ma20 = close_series.rolling(20).mean()
        std20 = close_series.rolling(20).std()
        upper_band = ma20 + 2 * std20
        lower_band = ma20 - 2 * std20
        
        curr_price = latest['收盘']
        curr_ma = ma20.iloc[-1]
        
        # 2. RSI 指标
        rsi_val = calculate_rsi(close_series).iloc[-1]
        
        # 3. 活跃度筛选 (20日平均振幅 > 1.2%)
        avg_amp = df['振幅'].tail(20).mean()
        
        # --- 战法决策逻辑 ---
        status = "常规震荡"
        action = "维持对称网格"
        multi_factor = "1.0x (等额)" # 加码系数
        
        if rsi_val > 70:
            status = "⚠️超买区"
            action = "暂停买入，逢高减仓"
        elif rsi_val < 30:
            status = "🔥超卖区"
            action = "暂停卖出，开启分级加码"
            multi_factor = "1.5x (加码)"
            
        boll_pos = "中轨上方(看强)" if curr_price > curr_ma else "中轨下方(看弱)"

        # 只要有基本成交量和波动就进入结果（不设太严，方便复盘）
        if avg_amp > 1.0 and latest['成交额'] > 0:
            code = os.path.basename(file_path).replace('.csv', '')
            return {
                '证券代码': code,
                '收盘价': curr_price,
                'RSI(14)': round(rsi_val, 2),
                '当前状态': status,
                '布林位置': boll_pos,
                '操作建议': action,
                '加码系数': multi_factor,
                '20日均振幅%': round(avg_amp, 2),
                '中轨(MA20)': round(curr_ma, 3)
            }
    except Exception:
        return None
    return None

def main():
    # 1. 加载映射文件
    name_map = {}
    if os.path.exists(ETF_LIST_FILE):
        name_df = pd.read_csv(ETF_LIST_FILE)
        name_map = dict(zip(name_df['证券代码'].astype(str), name_df['证券简称']))

    # 2. 扫描文件并并行分析
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"[{datetime.now()}] 正在并行分析 {len(csv_files)} 个基金数据...")
    
    with Pool(cpu_count()) as p:
        results = p.map(analyze_etf, csv_files)
    
    valid_results = [r for r in results if r is not None]
    
    if not valid_results:
        print("未发现满足波动率要求的标的。")
        return

    # 3. 构建结果表格
    final_df = pd.DataFrame(valid_results)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map.get(x, '未知'))
    
    # 排序列名
    cols = ['证券代码', '证券简称', '收盘价', 'RSI(14)', '当前状态', '布林位置', '操作建议', '加码系数', '20日均振幅%']
    final_df = final_df[cols]
    
    # 按振幅降序排列，优先展示活跃品种
    final_df = final_df.sort_values(by='20日均振幅%', ascending=False)
    
    # 4. 保存文件 (年月日目录)
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    
    file_name = f"etf_grid_hunter_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_path, file_name)
    
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"筛选成功！共选中 {len(final_df)} 只标的。")
    print(f"结果已存入: {save_path}")

if __name__ == "__main__":
    main()

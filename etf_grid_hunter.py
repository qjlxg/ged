import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：RSI-BOLL 动态增强网格战法
# 
# 核心逻辑（买卖逻辑与操作要领）：
# 1. RSI 择时：RSI > 70 属于超买区，网格应“只卖不买”；RSI < 30 属于超卖区，应“只买不卖”。
# 2. BOLL 动态中轴：利用布林带中轨作为网格基准。若价格在中轨上方，网格区间随之上移，防止踏空单边上涨。
# 3. 分级加码（马丁策略）：下跌时增加买入权重，摊薄成本；本脚本筛选出适合此操作的“宽幅震荡”标的。
# 4. 筛选标准：波动率适中 (振幅 > 1.5%)，成交额活跃，且处于 BOLL 轨道内运行。
# ==========================================

DATA_DIR = 'stock_data'
ETF_LIST_FILE = 'ETF列表.xlsx - Sheet1.csv'

def calculate_rsi(series, period=14):
    """计算 RSI 指标"""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_etf(file_path):
    """单体 ETF 技术分析（加入 RSI 和 BOLL）"""
    try:
        df = pd.read_csv(file_path)
        if len(df) < 30: return None
        
        df.columns = [c.strip() for c in df.columns]
        latest = df.iloc[-1]
        close_series = df['收盘']
        
        # --- 1. 计算 BOLL 指标 (20日) ---
        ma20 = close_series.rolling(20).mean()
        std20 = close_series.rolling(20).std()
        upper_band = ma20 + 2 * std20
        lower_band = ma20 - 2 * std20
        
        curr_price = latest['收盘']
        curr_ma20 = ma20.iloc[-1]
        
        # --- 2. 计算 RSI 指标 (14日) ---
        rsi = calculate_rsi(close_series).iloc[-1]
        
        # --- 3. 筛选逻辑：RSI-BOLL 增强型 ---
        # A. 活跃度筛选：20日平均振幅 > 1.5%
        avg_amplitude = df['振幅'].tail(20).mean()
        
        # B. 状态判定
        status = "区间震荡"
        action_advice = "维持正常网格"
        
        if rsi > 70:
            status = "超买/高位"
            action_advice = "暂停买入，仅执行卖出单"
        elif rsi < 30:
            status = "超卖/低位"
            action_advice = "暂停卖出，执行分级买入(加码)"
        
        # C. 动态中轴判定：价格在中轨之上，建议上移网格中轴
        boll_pos = "中轨下方"
        if curr_price > curr_ma20:
            boll_pos = "中轨上方"
            
        # 综合筛选条件：过滤极度不活跃标的 + 必须有成交量
        if avg_amplitude > 1.5 and latest['成交额'] > 1000000:
            code = os.path.basename(file_path).replace('.csv', '')
            return {
                '证券代码': code,
                '收盘价': curr_price,
                'RSI(14)': round(rsi, 2),
                '当前状态': status,
                '布林位置': boll_pos,
                '操作要领': action_advice,
                '20日均振幅%': round(avg_amplitude, 2),
                '中轨(MA20)': round(curr_ma20, 3)
            }
    except Exception as e:
        pass # 忽略格式错误的零星文件
    return None

def main():
    # 1. 映射名称
    name_map = {}
    if os.path.exists(ETF_LIST_FILE):
        name_df = pd.read_csv(ETF_LIST_FILE)
        name_map = dict(zip(name_df['证券代码'].astype(str), name_df['证券简称']))

    # 2. 并行处理
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    with Pool(cpu_count()) as p:
        results = p.map(analyze_etf, csv_files)
    
    valid_results = [r for r in results if r is not None]
    if not valid_results:
        return

    # 3. 整理输出
    final_df = pd.DataFrame(valid_results)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map.get(x, '未知'))
    
    # 调整列顺序
    cols = ['证券代码', '证券简称', '收盘价', 'RSI(14)', '当前状态', '布林位置', '操作要领', '20日均振幅%']
    final_df = final_df[cols]
    
    # 4. 保存
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, f"etf_grid_hunter_{now.strftime('%Y%m%d_%H%M%S')}.csv")
    
    final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"分析完成: {file_path}")

if __name__ == "__main__":
    main()

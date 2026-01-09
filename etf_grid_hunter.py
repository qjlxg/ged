import pandas as pd
import os
import glob
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==========================================
# 战法名称：低位高频震荡捕捉战法
# 买卖逻辑：
# 1. 筛选处于历史相对低位（近一年分位 < 50%）的 ETF，确保安全边际。
# 2. 筛选近期波动率（ATR/振幅）较高的品种，保证网格套利频繁触发。
# 3. 过滤掉成交量极低的僵尸基金。
# 操作要领：选出标的后，建议在中轨附近分批建仓，网格间距设为振幅的 1/2。
# ==========================================

DATA_DIR = 'stock_data'
ETF_LIST_FILE = 'ETF列表.xlsx - Sheet1.csv'

def analyze_etf(file_path):
    """单体 ETF 技术分析并行函数"""
    try:
        df = pd.read_csv(file_path)
        if len(df) < 60: return None
        
        # 统一列名处理（防止中英文干扰）
        df.columns = [c.strip() for c in df.columns]
        
        # 计算技术指标
        code = os.path.basename(file_path).replace('.csv', '')
        latest = df.iloc[-1]
        
        # 1. 计算近一年(250日)价格位置
        recent_250 = df.tail(250)
        low_250 = recent_250['最低'].min()
        high_250 = recent_250['最高'].max()
        price_pos = (latest['收盘'] - low_250) / (high_250 - low_250) if high_250 != low_250 else 1
        
        # 2. 计算平均振幅 (20日)
        avg_amplitude = df['振幅'].tail(20).mean()
        
        # 3. 20日均线趋势
        ma20 = df['收盘'].tail(20).mean()
        
        # --- 筛选条件 ---
        # A. 价格在一年内的中轴以下 (price_pos < 0.5)
        # B. 20日平均振幅 > 1.8% (网格活跃度)
        # C. 站上20日线 (短期转强)
        if price_pos < 0.5 and avg_amplitude > 1.8 and latest['收盘'] > ma20:
            return {
                '证券代码': code,
                '当前收盘': latest['收盘'],
                '年内位置%': round(price_pos * 100, 2),
                '20日均振幅%': round(avg_amplitude, 2),
                '成交额(万)': round(latest['成交额'] / 10000, 2)
            }
    except Exception as e:
        print(f"解析 {file_path} 出错: {e}")
    return None

def main():
    # 1. 加载 ETF 名称映射
    name_map = {}
    if os.path.exists(ETF_LIST_FILE):
        name_df = pd.read_csv(ETF_LIST_FILE)
        # 强制将代码转为带前缀的字符串或匹配格式
        name_map = dict(zip(name_df['证券代码'].astype(str), name_df['证券简称']))

    # 2. 并行扫描目录
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    print(f"开始扫描 {len(csv_files)} 个标的...")
    
    with Pool(cpu_count()) as p:
        results = p.map(analyze_etf, csv_files)
    
    # 3. 汇总结果
    valid_results = [r for r in results if r is not None]
    if not valid_results:
        print("今日无符合战法筛选条件的ETF")
        return

    final_df = pd.DataFrame(valid_results)
    # 匹配名称
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map.get(x, '未知'))
    
    # 4. 准备保存路径
    now = datetime.now() # GitHub Action 环境已处理时区
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    
    file_name = f"etf_grid_hunter_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    save_path = os.path.join(dir_path, file_name)
    
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"筛选完成，结果已保存至: {save_path}")

if __name__ == "__main__":
    main()

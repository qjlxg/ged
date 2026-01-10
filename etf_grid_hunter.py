import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime
from multiprocessing import Pool, cpu_count

# ==============================================================================
# 战法说明：RSI-BOLL-VOLUME 终极胜率增强版 (3万元实盘专用)
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
        # 读取 100 行确保 MA20 和 RSI 准确
        df = pd.read_csv(file_path, encoding='utf-8-sig').tail(100)
        if len(df) < 30: return None
        df.columns = [c.strip() for c in df.columns]
        
        latest = df.iloc[-1]
        close_series = df['收盘']
        
        # --- [逻辑 4 & 新 2：安全防护与换手率] ---
        turnover_raw = latest.get('换手率', 0)
        try:
            turnover = float(str(turnover_raw).replace('%', ''))
        except:
            turnover = 0
            
        # 严格执行 1000万成交额 + 0.1% 换手率过滤
        if latest['成交额'] < 10000000 or turnover < 0.1:
            return None

        # --- [技术指标计算] ---
        ma20_series = close_series.rolling(20).mean()
        ma20 = ma20_series.iloc[-1]
        rsi_val = calculate_rsi(close_series).iloc[-1]
        avg_amp = df['振幅'].tail(20).mean()
        
        # 新 3：乖离率 (Bias)
        bias = (latest['收盘'] - ma20) / ma20 * 100
        # 新 1：量比 (成交额协同)
        vol_ratio = latest['成交额'] / (df['成交额'].tail(5).mean() + 1e-9)

        # --- [新 4：横盘天数逻辑] ---
        # 判定标准：价格偏离 MA20 在 ±2% 范围内
        diff_pct = (close_series - ma20_series) / ma20_series
        is_sideways = diff_pct.abs() < 0.02
        sideways_days = 0
        for val in reversed(is_sideways.values):
            if val: sideways_days += 1
            else: break

        # --- [逻辑 2 & 5：风险锁与套利空间剔除] ---
        # RSI > 70 强制剔除 (逻辑 2)；振幅 < 1.2% 剔除 (逻辑 5)
        if rsi_val > 70 or avg_amp < 1.2:
            return None

        # --- [逻辑 1 & 3：状态判定] ---
        status = "正常震荡"
        action = "常规网格"
        weight = "1.0x"
        star = "★★★☆☆" 
        
        # 逻辑 1：动态中轴
        boll_pos = "中轨上方(看强)" if latest['收盘'] > ma20 else "中轨下方(看弱)"
        
        # 逻辑 2 & 3 & 新 1：机会区与金底判定
        if rsi_val < 35:
            status = "🔥机会区"
            if rsi_val < 30:
                status = "🚨超卖加码区"
                action = "暂停卖出/只买不卖"
                weight = "1.5x - 2.0x"
                star = "★★★★☆"
                
                # 新 1 + 新 3：量价协同金底
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
            '横盘天数': sideways_days,
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
    # 自动识别并加载白名单 (兼容 Excel 和多种编码的 CSV)
    if not os.path.exists(ETF_LIST_FILE):
        alt_csv = ETF_LIST_FILE.replace('.xlsx', '.csv')
        target_file = alt_csv if os.path.exists(alt_csv) else None
        if not target_file: return
    else:
        target_file = ETF_LIST_FILE

    try:
        if target_file.endswith('.xlsx'):
            name_df = pd.read_excel(target_file, engine='openpyxl')
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
    if not valid: return

    final_df = pd.DataFrame(valid)
    final_df['证券简称'] = final_df['证券代码'].apply(lambda x: name_map[x])
    
    # 按照置信度、横盘天数、RSI 排序
    cols = ['证券代码', '证券简称', '收盘价', '成交额(万)', 'RSI(14)', '量比', '乖离率%', '横盘天数',
            '网格状态', '胜率置信度', '布林位置', '建议操作', '加码倍数', '20日均振幅%']
    final_df = final_df[cols].sort_values(['胜率置信度', '横盘天数', 'RSI(14)'], ascending=[False, False, True])
    
    now = datetime.now()
    dir_path = now.strftime('%Y/%m')
    os.makedirs(dir_path, exist_ok=True)
    save_path = os.path.join(dir_path, f"best_buy_{now.strftime('%Y%m%d')}.csv")
    final_df.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"✅ 扫描完成：共发现 {len(final_df)} 个符合标准的标的。")

if __name__ == "__main__":
    main()

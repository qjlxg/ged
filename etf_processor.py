import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import os
import sys

def load_etf_data():
    try:
        df_sh = pd.read_excel('ETF列表沪.xls')
        df_sz = pd.read_excel('ETF列表深.xlsx')
        return df_sh, df_sz
    except Exception as e:
        print(f"数据加载失败: {e}")
        sys.exit(1)

def clean_and_deduplicate(df_sh, df_sz):
    # --- 1. 数据标准化与规模计算 ---
    # 沪市
    df_sh['规模'] = pd.to_numeric(df_sh['最新规模(亿元)'].astype(str).str.replace(',', ''), errors='coerce')
    df_sh = df_sh.rename(columns={'基金代码': '代码', '基金简称': '名称', '标的指数': '指数'})
    
    # 深市
    df_sz['规模'] = pd.to_numeric(df_sz['当前规模(份)'].astype(str).str.replace(',', ''), errors='coerce') / 100000000
    df_sz = df_sz.rename(columns={'证券代码': '代码', '证券简称': '名称', '拟合指数': '指数'})

    # 合并初筛
    df_all = pd.concat([df_sh[['代码', '名称', '指数', '规模']], df_sz[['代码', '名称', '指数', '规模']]], ignore_index=True)
    
    # --- 2. 关键词初滤 (去除非主流) ---
    exclude_keywords = ['增强', '指数基金', '退市', '分级', 'LOF', '联接', 'C类', 'E类']
    mask = ~df_all['名称'].astype(str).apply(lambda x: any(kw in x for kw in exclude_keywords))
    df_all = df_all[mask].copy()

    # --- 3. 核心：按指数去重 (保留规模最大者) ---
    # 清洗指数名称（去除空格，统一大小写）
    df_all['指数'] = df_all['指数'].astype(str).str.strip()
    
    # 对于有明确指数的品种，执行分组取最大
    # 过滤掉指数为 '-' 或 'nan' 或空的无效值
    valid_index_mask = ~df_all['指数'].isin(['-', 'nan', '', 'None'])
    
    df_with_index = df_all[valid_index_mask].copy()
    df_no_index = df_all[~valid_index_mask].copy()

    # 在同一指数中选规模最大的
    df_best_by_index = df_with_index.sort_values('规模', ascending=False).groupby('指数').head(1)
    
    # 合并结果
    df_final = pd.concat([df_best_by_index, df_no_index], ignore_index=True)
    
    # 二次兜底去重：如果名称非常接近（前4个字相同）且规模较小，则视为重复
    df_final = df_final.sort_values('规模', ascending=False)
    df_final['名称前缀'] = df_final['名称'].str[:4]
    df_final = df_final.drop_duplicates(subset=['名称前缀'], keep='first')

    return df_final

def save_results(df_final):
    # 格式化代码
    df_final['代码'] = df_final['代码'].astype(str).str.split('.').str[0].str.zfill(6)
    
    # 输出
    tz = pytz.timezone('Asia/Shanghai')
    ts = datetime.now(tz).strftime('%Y%m%d_%H%M%S')
    
    res = df_final[['代码', '名称']]
    res.to_csv("ETF列表.txt", sep='\t', index=False, encoding='utf-8-sig')
    res.to_excel("ETF列表.xlsx", index=False)
    
    # 备份带时间戳的文件
    res.to_csv(f"ETF列表_{ts}.txt", sep='\t', index=False, encoding='utf-8-sig')
    
    print(f"去重完成！同类产品已合并，最终保留 {len(res)} 个唯一主题的龙头ETF。")

if __name__ == "__main__":
    sh, sz = load_etf_data()
    final_df = clean_and_deduplicate(sh, sz)
    save_results(final_df)

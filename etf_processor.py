import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import os
import sys

def load_etf_data():
    """加载沪、深ETF数据"""
    try:
        # 读取沪市ETF数据
        df_sh = pd.read_excel('ETF列表沪.xls')
        print(f"沪市ETF加载成功: {len(df_sh)} 条")
        
        # 读取深市ETF数据
        df_sz = pd.read_excel('ETF列表深.xlsx')
        print(f"深市ETF加载成功: {len(df_sz)} 条")
        
        return df_sh, df_sz
    except Exception as e:
        print(f"数据加载失败, 请确保文件在根目录: {e}")
        sys.exit(1)

def clean_etf_data(df_sh, df_sz):
    # --- 沪市清理 ---
    if '最新规模(亿元)' in df_sh.columns:
        df_sh['最新规模(亿元)'] = pd.to_numeric(df_sh['最新规模(亿元)'].astype(str).str.replace(',', ''), errors='coerce')
        df_sh = df_sh[df_sh['最新规模(亿元)'] >= 1.0].copy()
    
    # --- 深市清理 ---
    if '当前规模(份)' in df_sz.columns:
        # 安全转换：去掉逗号，转为数值
        size_val = df_sz['当前规模(份)'].astype(str).str.replace(',', '')
        df_sz['当前规模(亿元)'] = pd.to_numeric(size_val, errors='coerce') / 100000000
        df_sz = df_sz[df_sz['当前规模(亿元)'] >= 1.0].copy()

    # --- 关键词筛选 (精准化) ---
    # 删除了 "基金" 关键词，因为它太容易误杀正常品种
    exclude_keywords = ['增强', '指数基金', '退市', '分级']
    
    def refine_filter(df, name_col):
        # 只在名称确实包含黑名单词汇时剔除
        mask = ~df[name_col].astype(str).apply(
            lambda x: any(kw in x for kw in exclude_keywords)
        )
        return df[mask]

    # 执行筛选
    res_sh = refine_filter(df_sh, '基金简称' if '基金简称' in df_sh.columns else '证券简称')
    res_sz = refine_filter(df_sz, '证券简称')

    print(f"筛选后：沪市 {len(res_sh)} 条，深市 {len(res_sz)} 条")
    return res_sh, res_sz

def save_results(df_sh, df_sz):
    # 统一字段
    sh_final = df_sh[['基金代码', '基金简称']].rename(columns={'基金代码': '代码', '基金简称': '名称'})
    sz_final = df_sz[['证券代码', '证券简称']].rename(columns={'证券代码': '代码', '证券简称': '名称'})
    
    df_all = pd.concat([sh_final, sz_final], ignore_index=True)
    df_all['代码'] = df_all['代码'].astype(str).str.split('.').str[0].str.zfill(6)
    df_all = df_all.drop_duplicates(subset=['代码'])

    # 时间戳 (上海)
    tz = pytz.timezone('Asia/Shanghai')
    ts = datetime.now(tz).strftime('%Y%m%d_%H%M%S')

    # 保存
    df_all.to_csv(f"ETF列表_{ts}.txt", sep='\t', index=False, encoding='utf-8-sig')
    df_all.to_excel(f"ETF列表_{ts}.xlsx", index=False)
    df_all.to_csv("ETF列表.txt", sep='\t', index=False, encoding='utf-8-sig')
    df_all.to_excel("ETF列表.xlsx", index=False)
    print(f"保存成功，总计 {len(df_all)} 个核心品种")

if __name__ == "__main__":
    sh, sz = load_etf_data()
    sh_c, sz_c = clean_etf_data(sh, sz)
    save_results(sh_c, sz_c)

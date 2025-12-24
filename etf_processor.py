#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF列表数据处理脚本
功能：读取沪、深ETF列表，清理流动性差、规模小的品种，专注于主流、易于交易的ETF
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys

def load_etf_data():
    """加载沪、深ETF数据"""
    try:
        # 读取沪市ETF数据
        df_sh = pd.read_excel('ETF列表沪.xls')
        print(f"沪市ETF数据加载成功，共 {len(df_sh)} 条记录")
        
        # 读取深市ETF数据
        df_sz = pd.read_excel('ETF列表深.xlsx')
        print(f"深市ETF数据加载成功，共 {len(df_sz)} 条记录")
        
        return df_sh, df_sz
    except FileNotFoundError as e:
        print(f"文件未找到: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"数据加载失败: {e}")
        sys.exit(1)

def clean_etf_data(df_sh, df_sz):
    """
    清理ETF数据，去除流动性差、规模小的品种
    筛选规则：
    1. 去除最新规模小于1亿元的品种
    2. 去除名称中包含"增强"、"指数基金"等非核心字段的冗余产品
    3. 优先保留主流宽基、行业ETF和跨境ETF
    """
    
    # 沪市ETF清理
    # 转换规模列为数值类型
    if '最新规模(亿元)' in df_sh.columns:
        df_sh['最新规模(亿元)'] = pd.to_numeric(df_sh['最新规模(亿元)'], errors='coerce')
        # 筛选规模大于1亿元的ETF
        df_sh_filtered = df_sh[df_sh['最新规模(亿元)'] >= 1].copy()
        print(f"沪市ETF规模筛选后剩余 {len(df_sh_filtered)} 条记录")
    else:
        df_sh_filtered = df_sh.copy()
    
    # 深市ETF清理
    if '当前规模(份)' in df_sz.columns:
        # 深市数据单位是"份"，转换为亿元（假设1份=1元）
        df_sz['当前规模(亿元)'] = df_sz['当前规模(份)'] / 100000000
        df_sz_filtered = df_sz[df_sz['当前规模(亿元)'] >= 1].copy()
        print(f"深市ETF规模筛选后剩余 {len(df_sz_filtered)} 条记录")
    else:
        df_sz_filtered = df_sz.copy()
    
    # 去除名称冗余的ETF
    exclude_keywords = ['增强', '指数基金', '基金', 'ETF基金', 'ETF指数']
    
    def filter_by_name(df, name_col):
        """根据名称关键词筛选"""
        mask = ~df[name_col].astype(str).apply(
            lambda x: any(keyword in x for keyword in exclude_keywords)
        )
        return df[mask]
    
    # 沪市名称筛选
    if '基金简称' in df_sh_filtered.columns:
        df_sh_cleaned = filter_by_name(df_sh_filtered, '基金简称')
    elif '证券简称' in df_sh_filtered.columns:
        df_sh_cleaned = filter_by_name(df_sh_filtered, '证券简称')
    else:
        df_sh_cleaned = df_sh_filtered
    
    # 深市名称筛选
    if '证券简称' in df_sz_filtered.columns:
        df_sz_cleaned = filter_by_name(df_sz_filtered, '证券简称')
    else:
        df_sz_cleaned = df_sz_filtered
    
    print(f"沪市ETF名称筛选后剩余 {len(df_sh_cleaned)} 条记录")
    print(f"深市ETF名称筛选后剩余 {len(df_sz_cleaned)} 条记录")
    
    return df_sh_cleaned, df_sz_cleaned

def categorize_etfs(df_sh, df_sz):
    """对ETF进行分类整理"""
    
    # 合并数据
    all_etfs = []
    
    # 处理沪市ETF
    for _, row in df_sh.iterrows():
        etf_info = {
            '证券代码': row.get('基金代码', row.get('证券代码', '')),
            '证券简称': row.get('基金简称', row.get('证券简称', '')),
            '标的指数': row.get('标的指数', ''),
            '最新规模(亿元)': row.get('最新规模(亿元)', ''),
            '基金管理人': row.get('基金管理人', ''),

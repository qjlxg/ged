import pandas as pd
import os
from datetime import datetime
import pytz

def process_etf():
    # 1. 设置上海时区
    tz = pytz.timezone('Asia/Shanghai')
    timestamp = datetime.now(tz).strftime('%Y%m%d_%H%M%S')
    
    # 2. 定义过滤规则
    # 排除非核心或流动性可能较差的冗余品种
    blacklist = ['增强', '指数基金', '退市', '分级', 'C', 'E']
    # 核心关注类别（用于在名称中匹配）
    core_keywords = ['创业板', '科创', '沪深300', '上证50', '中证500', '中证1000', '纳指', '恒生', '医疗', '半导体', '新能源', '芯片']

    def clean_df(df, code_col, name_col, size_col, is_shares=False):
        # 转换规模：深市如果是“份”，假设净值约1元进行初步过滤（或直接按1亿份过滤）
        df[size_col] = df[size_col].astype(str).str.replace(',', '').astype(float)
        
        # 规则1：规模过滤 (>= 1亿)
        # 沪市单位是亿元，深市单位是份（1亿份通常对应1亿左右规模）
        df = df[df[size_col] >= 1.0 if not is_shares else df[size_col] >= 100000000]
        
        # 规则2：关键词过滤 (去除增强、非核心字段)
        mask_black = df[name_col].str.contains('|'.join(blacklist), na=False)
        df = df[~mask_black]
        
        # 规则3：确保名称不为空
        df = df.dropna(subset=[name_col])
        
        return df[[code_col, name_col]].rename(columns={code_col: '代码', name_col: '名称'})

    print("正在读取并清理数据...")

    # 处理沪市
    try:
        df_hu = pd.read_excel('ETF列表沪.xls')
        res_hu = clean_df(df_hu, '基金代码', '基金简称', '最新规模(亿元)', is_shares=False)
    except Exception as e:
        print(f"沪市处理跳过: {e}")
        res_hu = pd.DataFrame()

    # 处理深市
    try:
        df_shen = pd.read_excel('ETF列表深.xlsx')
        res_shen = clean_df(df_shen, '证券代码', '证券简称', '当前规模(份)', is_shares=True)
    except Exception as e:
        print(f"深市处理跳过: {e}")
        res_shen = pd.DataFrame()

    # 3. 合并数据
    df_final = pd.concat([res_hu, res_shen], ignore_index=True)
    df_final['代码'] = df_final['代码'].astype(str).str.zfill(6)
    df_final = df_final.drop_duplicates(subset=['代码'])

    # 4. 保存结果
    txt_output = f"ETF列表_{timestamp}.txt"
    xlsx_output = f"ETF列表_{timestamp}.xlsx"
    
    df_final.to_csv(txt_output, sep='\t', index=False, encoding='utf-8')
    df_final.to_excel(xlsx_output, index=False)
    
    # 覆盖简易名称以便其他程序读取
    df_final.to_csv("ETF列表.txt", sep='\t', index=False, encoding='utf-8')
    df_final.to_excel("ETF列表.xlsx", index=False)

    print(f"清理完成。输出文件: {txt_output}")

if __name__ == "__main__":
    process_etf()

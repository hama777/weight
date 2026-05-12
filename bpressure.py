#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import locale
import pandas as pd
import datetime 
import subprocess
from ftplib import FTP_TLS

# 26/05/12 v1.00 移動平均を全データとした
version = "1.00" 
debug = 0
appdir = os.path.dirname(os.path.abspath(__file__))

templatefile = appdir + "./press_templ.htm"
resultfile = appdir + "./press.htm"
conffile = appdir + "./weight.conf"
datafile = appdir + "./体重.xls"     # debug用  本番用は confファイルで設定

month_avarage_df = ""
month_table_col = 0     # 月テーブルの列
prev_diff = -1 
rank_month_average_count = 0

#  maxmin
#  最大最小値のデータ  辞書型  key  mean,std,max,min  value  辞書型 key max,min,maxyymm,minyymm
#  maxmin_data   mean  max 最大値
#                      min 最小値
#                      maxyymm  最大値のyymm
#                      minyymm  最小値
#                std   max 最大値   ..... 
maxmin = {}     # 
key_list = ["mean","std","max","min"]

def main_proc():
    global current_yymm

    locale.setlocale(locale.LC_TIME, '')
    date_settings()
    read_config()
    read_data()
    #calc_statistics()
    #create_month_ave_diff()
    #create_day_diff()
    parse_template()
    if debug == 1 :
        return
    result = subprocess.run((browser, resultfile))

def read_config() :
    global ftp_host,ftp_user,ftp_pass,ftp_url,datafile,browser,pixela_url,pixela_token,debug
    if not os.path.isfile(conffile) :
        debug = 1 
        return
    conf = open(conffile,'r', encoding='utf-8')
    datafile = conf.readline().strip()
    browser = conf.readline().strip()
    ftp_host = conf.readline().strip()
    ftp_user = conf.readline().strip()
    ftp_pass = conf.readline().strip()
    ftp_url = conf.readline().strip()
    #pixela_url = conf.readline().strip()
    #pixela_token = conf.readline().strip()
    conf.close()

def read_data() :
    global df_pressure
    df_pressure = pd.read_excel(datafile,sheet_name ='血圧',usecols=range(5),
                       header = 1, names=["pdate", "m_high","m_low","e_high","e_low",])  # 0,1 カラムのみ読み込み
    
    #df_pressure = df_pressure.dropna()
    df_pressure = df_pressure.dropna(
        subset=["m_high", "m_low", "e_high", "e_low"],
        how="all"
    )
    df_pressure['pdate'] = pd.to_datetime(df_pressure['pdate'])
    df_pressure["ave_high"] = df_pressure[["m_high", "e_high"]].mean(axis=1)
    df_pressure["ave_low"]  = df_pressure[["m_low", "e_low"]].mean(axis=1)
    df_pressure['week_high'] = (
        df_pressure.set_index('pdate')['ave_high']
            .rolling(7)
            .mean()
            .reset_index(drop=True)
    )
    df_pressure['week_low'] = (
        df_pressure.set_index('pdate')['ave_low']
            .rolling(7)
            .mean()
            .reset_index(drop=True)
    )
    #print(df_pressure.tail(20))

def month3_graph() :
    df_qu = df_pressure.tail(90)
    for index, row in df_qu.iterrows():
        dt = row['pdate']

        out.write(f"['{dt.month}/{dt.day}',{row['ave_high']},{row['ave_low']}],") 

def week_ave_graph() :
    df_qu = df_pressure 
    for index, row in df_qu.iterrows():
        if pd.isna(row['week_high']) or pd.isna(row['week_low'])  :
            continue

        dt = row['pdate']
        out.write(f"['{dt.month}/{dt.day}',{row['week_high']},{row['week_low']}],") 

def month_info() :

    # 月ごとに集計
    df_mon = (
        df_pressure
        .assign(month=df_pressure['pdate'].dt.to_period('M'))
        .groupby('month')
        .agg({
            'ave_high': ['mean', 'max', 'min', 'std'],
            'ave_low':  ['mean', 'max', 'min', 'std']
        })
    )

    # カラム名をフラットにする（任意）
    df_mon.columns = ['_'.join(col) for col in df_mon.columns]

    # 必要なら month を通常の datetime に戻す
    df_mon = df_mon.reset_index()
    df_mon['month'] = df_mon['month'].dt.to_timestamp()

    for index, row in df_mon.iterrows():
        yymm = row['month'].strftime("%y/%m")
        out.write(f"<tr><td>{yymm}</td><td align=right>{row['ave_high_mean']:7.1f}</td><td align=right>{row['ave_high_max']:7.0f}</td>"
                  f"<td align=right>{row['ave_high_min']:7.0f}</td><td align=right>{row['ave_high_std']:7.1f}</td>"
                  f"<td align=right>{row['ave_low_mean']:7.1f}</td><td align=right>{row['ave_low_max']:7.0f}</td>"
                  f"<td align=right>{row['ave_low_min']:7.0f}</td><td align=right>{row['ave_low_std']:7.1f}</td>"
                  f"</tr>\n")


def date_settings():
    global  today_date,today_mm,today_dd,today_yy,lastdate,today_datetime,today_yymm
    today_datetime = datetime.datetime.today()
    today_date = datetime.date.today()
    today_mm = today_date.month
    today_dd = today_date.day
    today_yy = today_date.year
    today_yymm = (today_yy - 2000)  * 100 + today_mm  # yymm の形式にする

def today(s):
    d = today_datetime.strftime("%m/%d %H:%M")
    s = s.replace("%today%",d)
    out.write(s)

def parse_template() :
    global out ,lastdate,prev_day
    f = open(templatefile , 'r', encoding='utf-8')
    out = open(resultfile,'w' ,  encoding='utf-8')
    for line in f :
        if "%month3_graph" in line :
            month3_graph()
            continue
        if "%week_ave_graph" in line :
            week_ave_graph()
            continue
        if "%month_info%" in line :
            month_info()
            continue
        if "%lastdate%" in line :
            lastdate_datetime = df['wdate'].iloc[-1]
            lastdate = lastdate_datetime.strftime('%y/%m/%d')
            line = line.replace("%lastdate%",lastdate)
            out.write(line)
            #  prev_day で rank_com 等で使用
            prev_day = lastdate_datetime - datetime.timedelta(days=1)
            prev_day = prev_day.strftime('%y/%m/%d')
            continue
        if "%version%" in line :
            s = line.replace("%version%",version)
            out.write(s)
            continue
        if "%today%" in line :
            today(line)
            continue

        out.write(line)

    f.close()
    out.close()

# ----------------------------------------------------------
main_proc()

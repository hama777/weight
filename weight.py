#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import locale
import pandas as pd
import datetime 
import subprocess
from ftplib import FTP_TLS

version = "1.04"      #  24/06/24
debug = 0
appdir = os.path.dirname(os.path.abspath(__file__))

templatefile = appdir + "./wei_templ.htm"
resultfile = appdir + "./weight.htm"
conffile = appdir + "./weight.conf"

month_avarage_df = ""
month_table_col = 0     # 月テーブルの列
prev_diff = -1 
rank_month_average_count = 0
current_yymm = 0

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
    now = datetime.datetime.now()
    current_year = now.year
    current_month = now.month
    current_yymm = current_year * 100 + current_month - 200000   # yymm の形式にする

    read_config()
    read_data()
    calc_statistics()
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

#  年、月ごとの統計量を求め df を作成する
def calc_statistics() :
    global df_monstat,df_yearstat
    column_names = ['yymm', 'mean', 'median','std','max','min']
    df_monstat = pd.DataFrame(columns=column_names)
    df_yearstat = pd.DataFrame(columns=column_names)
    for yy in range(2010, 2025) : 
        dfyy = df[df['wdate'].dt.year == yy]
        m = dfyy.mean()['weight']
        if pd.isna(m):
            break

        tmp_list = []
        tmp_list.append(yy)
        tmp_list.append(m)
        tmp_list.append(dfyy.median()['weight'])
        tmp_list.append(dfyy.std()['weight'])
        tmp_list.append(dfyy.max()['weight'])
        tmp_list.append(dfyy.min()['weight'])
        tmp_df = pd.DataFrame([tmp_list],columns=column_names)
        df_yearstat = pd.concat([df_yearstat, tmp_df])

        for mm in range(1,13) :
            dfmm = dfyy[dfyy['wdate'].dt.month == mm]
            m = dfmm.mean()['weight']
            if pd.isna(m):
                break
            tmp_list = []
            tmp_list.append(yy*100+mm)
            tmp_list.append(m)
            tmp_list.append(dfmm.median()['weight'])
            tmp_list.append(dfmm.std()['weight'])
            tmp_list.append(dfmm.max()['weight'])
            tmp_list.append(dfmm.min()['weight'])
            tmp_df = pd.DataFrame([tmp_list],columns=column_names)

            df_monstat = pd.concat([df_monstat, tmp_df])
            df_monstat.reset_index(drop=True,inplace=True)

def summary() :
    global maxmin
    ave = df_monstat['mean'].iloc[-1]
    prev_ave = df_monstat['mean'].iloc[-2]
    std = df_monstat['std'].iloc[-1]
    max = df_monstat['max'].iloc[-1]
    prev_max = df_monstat['max'].iloc[-2]
    min = df_monstat['min'].iloc[-1]
    prev_min = df_monstat['min'].iloc[-2]
    rank = int(df_monstat.rank()['mean'].iloc[-1])
    count = len(df_monstat)

    for k in key_list :
        maxmin_data = {}
        maxmin_data['max'] = df_monstat[k].max()
        ix = df_monstat[k].idxmax()
        maxmin_data['maxyymm']  = df_monstat['yymm'].loc[ix]  - 200000
        maxmin_data['min']  = df_monstat[k].min()
        ix = df_monstat[k].idxmin()
        maxmin_data['minyymm'] = df_monstat['yymm'].loc[ix]  - 200000
        maxmin[k] = maxmin_data

    out.write(f'<tr><td>今月(前月差)</td><td>{ave:7.2f}({ave -prev_ave:.2f})</td><td>{rank}/{count}</td>'
              f'<td>{std:7.3f}</td><td>{max}({max-prev_max:.1f}) </td><td>{min}({min-prev_min:.1f})</td></tr>')
    out.write(f'<tr><td>最高(年月)</td><td>{maxmin["mean"]["max"]:7.2f}({maxmin["mean"]["maxyymm"]})</td><td>--</td>'
              f'<td>{maxmin["std"]["max"]:7.3f}({maxmin["std"]["maxyymm"]})</td><td>{maxmin["max"]["max"]:.1f}({maxmin["max"]["maxyymm"]})</td>'
              f'<td>{maxmin["min"]["max"]:.1f}({maxmin["min"]["maxyymm"]})</td></tr>')
    out.write(f'<tr><td>最低(年月)</td><td>{maxmin["mean"]["min"]:7.2f}({maxmin["mean"]["minyymm"]})</td><td>--</td>'
              f'<td>{maxmin["std"]["min"]:7.3f}({maxmin["std"]["minyymm"]})</td><td>{maxmin["max"]["min"]:.1f}({maxmin["max"]["minyymm"]})</td>'
              f'<td>{maxmin["min"]["min"]:.1f}({maxmin["min"]["minyymm"]})</td></tr>')

#  月平均ランキング  
def rank_month_average_high() :
    sorted_month_avarage_df  = df_monstat.sort_values('mean',ascending=False)
    monrank = sorted_month_avarage_df.head(20)   
    rank_month_average_com(monrank)

def rank_month_average_low() :
    sorted_month_avarage_df  = df_monstat.sort_values('mean',ascending=True)
    monrank = sorted_month_avarage_df.head(20)   
    rank_month_average_com(monrank)

#  月平均ランキング  共通
def rank_month_average_com(df_rank) :
    global rank_month_average_count
    rank_month_average_count = rank_month_average_count + 1  
    i = 0
    for index, row in df_rank.iterrows():
        i = i+1 
        if rank_month_average_count == 1 :
            if i > 10 :
                break
        if rank_month_average_count == 2 :
            if i <= 10 :
                continue
        yymm = row["yymm"] - 200000
        str_yymm = yymm 
        str_mean = f'{row["mean"]:7.2f}'
        if yymm == current_yymm :
            str_yymm = f'<span class=red>{str_yymm}</span>'
            str_mean = f'<span class=red>{str_mean}</span>'

        out.write(f'<tr><td align="right">{i}</td><td align="right">{str_yymm}</td>'
                  f'<td>{str_mean}</td></tr>')
    if rank_month_average_count == 2 :
        rank_month_average_count = 0

#  月別情報
def month_table() :
    global month_table_col,prev_diff
    month_table_col = month_table_col + 1   # 現在のカラム
    num_col = 12 * 4    #  1カラムの月数
    start = (month_table_col -1 ) * num_col + 1 
    end = month_table_col * num_col

    n = 0 
    for _,wdata in df_monstat.iterrows():
        n = n + 1 
        if n < start or n > end :
            continue 
        if prev_diff == -1 :
            diff = 0 
        else :
            diff = wdata["mean"] - prev_diff 
        if diff < 0 :
            diff_str = f"<span class=red>{diff:7.2f}</span>"
        else :
            diff_str = f"{diff:7.2f}"
        prev_diff = wdata['mean']
        yymm = wdata['yymm'] - 200000

        mean_str = set_css(f'{wdata["mean"]:7.2f}',"mean",yymm)
        std_str = set_css(f'{wdata["std"]:7.3f}',"std",yymm)
        max_str = set_css(f'{wdata["max"]:7.1f}',"max",yymm)
        min_str = set_css(f'{wdata["min"]:7.1f}',"min",yymm)

        out.write(f'<tr><td align="right">{yymm}</td><td align="right">{mean_str}</td>'
                  f'<td align="right">{diff_str}</td><td align="right">{std_str}</td>'
                  f'<td>{max_str}</td><td>{min_str}</td></tr>')

#   max min の場合はcssを設定する
def set_css(s,cate,yymm) :
    target_str = s
    if maxmin[cate]["maxyymm"] == yymm :
        target_str = f'<span class=max>{s}</span>'
    if maxmin[cate]["minyymm"] == yymm :
        target_str = f'<span class=min>{s}</span>'
    return target_str

#  年別情報
def year_table() :
    prev = -1
    for _,wdata in df_yearstat.iterrows(): 
        if prev == -1 :
            diff = 0 
        else :
            diff = wdata["mean"] - prev 
        if diff < 0 :
            diff_str = f"<span class=red>{diff:7.2f}</span>"
        else :
            diff_str = f"{diff:7.2f}"
        prev = wdata['mean']
        yy = wdata["yymm"]
        out.write(f'<tr><td align="right">{yy}</td><td align="right">{wdata["mean"]:7.3f}</td>'
                  f'<td align="right">{diff_str}</td><td align="right">{wdata["std"]:7.3f}</td>'
                  f'<td>{wdata["max"]:7.1f}</td><td>{wdata["min"]:7.1f}</td></tr>')

#  月別平均グラフ
def month_ave_graph() :
    for _,wdata in df_monstat.iterrows(): 
        m = wdata['mean']
        yymm = wdata['yymm'] - 200000   # 西暦を2桁表示にする
        out.write(f"['{yymm}',{m}],") 

def read_data() :
    global df , datafile
    if debug == 1 :
        datafile = appdir + "./体重debug.xls"
    df = pd.read_excel(datafile,sheet_name ='体重',usecols=[0, 1],
                       header = 1, names=["wdate", "weight",])  # 0,1 カラムのみ読み込み
    df = df.dropna()
    df['wdate'] = pd.to_datetime(df['wdate'])


#  体重グラフ(90日)
def month3_graph() :
    df_mon = df.tail(90)
    for index, row in df_mon.iterrows():
        dt = row['wdate']

        out.write(f"['{dt.month}/{dt.day}',{row['weight']}],") 

#  体重移動平均グラフ(1年)
def mvave_graph() :
    priod = 365
    mov_ave_dd = 7 
    df_yy = df.tail(priod+mov_ave_dd)
    df_yy['weight'] = df_yy['weight'].rolling(mov_ave_dd).mean()
    df_yy = df_yy.tail(priod)
    #print(df_yy)
    for index,row  in df_yy.iterrows() :
        dt = row["wdate"]
        out.write(f"['{dt.month}/{dt.day}',{row['weight']:5.2f}],") 

#  ランキング
def rank_month_top() :
    df_mon = df.tail(30)
    rank_common(df_mon,False,1)

def rank_year_top():
    df_mon = df.tail(365)
    rank_common(df_mon,False,1)

def rank_all_top(half):
    rank_common(df,False,half)

def rank_month_bottom() :
    df_mon = df.tail(30)
    rank_common(df_mon,True,1)

def rank_year_bottom() :
    df_mon = df.tail(365)
    rank_common(df_mon,True,1)

def rank_all_bottom(half) :
    rank_common(df,True,half)

def rank_common(rankdf,flg_ascending,half):
    sorted  = rankdf.sort_values('weight',ascending=flg_ascending)
    if half == 1 :
        monrank = sorted.head(10)   
    else :
        monrank = sorted.head(20)   

    i = 0
    for index, row in monrank.iterrows():
        i = i+1 
        if half == 2 :
            if i <= 10 :
                continue 
        date_str = row["wdate"].strftime("%y/%m/%d")
        date_color = row["wdate"].strftime("%y/%m/%d (%a)")
        if date_str == lastdate :
            date_color = f'<span class=red>{date_color}</span>'
        if date_str == prev_day :
            date_color = f'<span class=blue>{date_color}</span>'
        out.write(f'<tr><td align="right">{i}</td><td align="right">{row["weight"]}</td>'
                  f'<td>{date_color}</td></tr>')

def today(s):
    d = datetime.datetime.today().strftime("%m/%d %H:%M")
    s = s.replace("%today%",d)
    out.write(s)

def parse_template() :
    global out ,lastdate,prev_day
    f = open(templatefile , 'r', encoding='utf-8')
    out = open(resultfile,'w' ,  encoding='utf-8')
    for line in f :
        if "%summary" in line :
            summary()
            continue
        if "%month3_graph" in line :
            month3_graph()
            continue
        if "%mvave_graph" in line :
            mvave_graph()
            continue
        if "%rank_month_top" in line :
            rank_month_top()
            continue
        if "%rank_year_top" in line :
            rank_year_top()
            continue
        if "%rank_all_top1" in line :
            rank_all_top(1)
            continue
        if "%rank_all_top2" in line :
            rank_all_top(2)
            continue
        if "%rank_month_bottom" in line :
            rank_month_bottom()
            continue
        if "%rank_year_bottom" in line :
            rank_year_bottom()
            continue
        if "%rank_all_bottom1" in line :
            rank_all_bottom(1)
            continue
        if "%rank_all_bottom2" in line :
            rank_all_bottom(2)
            continue
        if "%month_ave_graph" in line :
            month_ave_graph()
            continue
        if "%year_table" in line :
            year_table()
            continue
        if "%month_table" in line :
            month_table()
            continue
        if "%rank_month_average_low" in line :
            rank_month_average_low()
            continue
        if "%rank_month_average_high" in line :
            rank_month_average_high()
            continue
        if "%lastdate%" in line :
            lastdate_datetime = df['wdate'].iloc[-1]
            lastdate = lastdate_datetime.strftime('%y/%m/%d')
            prev_day = lastdate_datetime - datetime.timedelta(days=1)
            prev_day = prev_day.strftime('%y/%m/%d')
            line = line.replace("%lastdate%",lastdate)
            out.write(line)
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

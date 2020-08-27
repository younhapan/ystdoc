# -*- coding: utf-8 -*-
import requests
import json
import bytedtqs as tqs
import pandas as pd
import sys
from datetime import date, timedelta, datetime
from pandas.tseries.offsets import DateOffset
from sortedcontainers import SortedDict, SortedSet

DEBUG = True
DEBUG_WEBHOOK = 'b8ea27b07daf43dd82d794b7491ad37c'

WEBHOOKS = [
    'b8ea27b07daf43dd82d794b7491ad37c',  # 协作空间
    # 'eb3f716d6e4e4a65bbc1cad65bd39fc9',
    'cfaa3cb4e6bd4d69a066b2f3e57d9a65'  # 数据勾搭小群
]

DETAILS_HOOK = [
    "b8ea27b07daf43dd82d794b7491ad37c"
    # 'eb3f716d6e4e4a65bbc1cad65bd39fc9',
    # 'cfaa3cb4e6bd4d69a066b2f3e57d9a65'  # 数据勾搭小群
]

TQS_DICT = {
    "appid": "LIkpe4p6hJGmREgHkj4pxAT5mNmKSnawK5WEGyFPgENWHtB7",
    "appkey": "fWqzX67iRCT4DjY8ctmrG5KcCIv57ahu6dyYMpG51EIUsVFX"
}
# 这里需要按照表结构复制进来
COLS_TUPLE = [('抖→火日新增投稿数', 6), ('抖→火日新增可推荐投稿数', 0), ('抖→火召回期投稿数', 1),
              ('抖→火召回期可推荐投稿数', 0), ('抖→火日新增投稿当日火山端vv', 8),
              ('抖→火召回期投稿当日火山端vv', 3), ('抖→火日新增投稿当日抖音端vv', 7),
              ('抖→火召回期投稿当日抖音端vv', 2), ('火山端当日总vv', 0), ('召回期高热火山端当日总vv', 0),
              ('召回期高热总投稿数(火山+抖→火)', 0), ('抖→火召回期高热当日总vv', 0),
              ('抖→火召回期高热总投稿数', 0), ('抖→火召回期投稿当日火山端展现uv', 0),
              ('抖→火召回期投稿当日火山端播放uv', 0), ('火→抖日新增投稿数', 4), ('火→抖召回期投稿数', 1),
              ('火→抖日新增投稿当日抖音端vv', 6), ('火→抖日新增投稿当日火山端vv', 5),
              ('火→抖召回期投稿当日抖音端vv', 3), ('火→抖召回期投稿当日火山端vv', 2), ('日期', 0)]

COMPUTE_COLS = SortedDict({
    "抖→火召回期投稿vv占当日火山端vv占比": ("抖→火召回期投稿当日火山端vv", "火山端当日总vv", 4),
    "抖→火召回期高热vv占当日火山端高热vv占比": ("抖→火召回期高热当日总vv", "召回期高热火山端当日总vv", 5)
})

AW2HS = [(each[0], each[1]) for each in COLS_TUPLE
         if each[0].startswith('抖→火') and each[1] > 0]
HS2AW = [(each[0], each[1]) for each in COLS_TUPLE
         if each[0].startswith('火→抖') and each[1] > 0]
for k, v in COMPUTE_COLS.items():
    AW2HS.append((k, v[2]))
AW2HS.sort(key=lambda x: x[1])
HS2AW.sort(key=lambda x: x[1])

# SQL
SQL = """
select * from ies_hotsoon_bgpm.rpt_vcd_items_stats_operation_day
where `date` >= '${date-14}'
"""


def post_to_lark(title, text, hook):
    url = f'https://open.feishu.cn/open-apis/bot/hook/{hook}'
    s = requests.post(url, data=json.dumps({"title": title, "text": text}))
    return s.json()


def number_formatter_helper(num):
    if 10e+6 <= num <= 10e+7:
        return "{x:.0f}万".format(x=num / 10e+3)
    elif num > 10e+7:
        return "{x:.2f}亿".format(x=num / 10e+7)
    elif 10e+3 <= num <= 10e+6:
        return "{x:.2f}万".format(x=num / 10e+3)
    else:
        return f"{num}"


def run_sql(sql):
    job_conf = {
        "yarn.cluster.name": "topi",
        "mapreduce.job.queuename": "root.topi_huoshan.pmb",
    }
    sql_conf = """
    set tqs.fallback.option=true;
    """
    client = tqs.TQSClient(app_id=TQS_DICT["appid"],
                           app_key=TQS_DICT["appkey"],
                           cluster="cn")
    job = client.execute_query(user_name="dufan",
                               query=(sql_conf + sql).strip(),
                               conf=job_conf)
    if job.is_success():
        results = job.get_result().fetch_all_data()
        return True, results  # results 第一行是header，第2行开始是数据
    else:
        return False, None


sql_data = run_sql(SQL)
if not sql_data[0]:
    print('执行失败...')
    sys.exit(1)
else:
    data = sql_data[1]
    df = pd.DataFrame(data[1:], columns=[x[0] for x in COLS_TUPLE])
    df.fillna(0, inplace=True)
    df.replace({'NULL': 0}, inplace=True)  # dirty hack. TQS 返回的NULL会被解析为字符串
    for x in df.columns:
        if x != "日期":
            df[x] = df[x].astype(int)
        else:
            df[x] = pd.to_datetime(df[x], format='%Y%m%d')

for k, v in COMPUTE_COLS.items():  # 计算指标
    df[k] = df[v[0]] / df[v[1]]
# df.sort_values(by='日期', inplace=True)

df.set_index('日期', inplace=True)
df.sort_index(inplace=True)
# 日环比
diff_day = df.pct_change(periods=1, freq='D')
# 周同比
diff_week = df.pct_change(periods=7, freq='D')
# 日绝对值变化
diff_day_abs = df.diff(periods=1)

# 周同比绝对值变化
diff_week_abs = df.diff(periods=7)
#
week_avg = df.groupby(pd.Grouper(freq='W-TUE')).mean().reset_index()  # 周四起始THU,周三起始WED。计算本周三到下周二为一周
week_avg.set_index('日期', inplace=True)

this_day = datetime.strptime("${date}", '%Y%m%d')
offset = pd.offsets.Week(weekday=2)  # 周四起始=3，周三起始=2

# 下一个周四，上一个周四
next_thursday, last_thursday = offset.rollforward(this_day), offset.rollback(
    this_day)
this_week_wtd = week_avg.loc[next_thursday]
last_week = week_avg.loc[last_thursday]

# 天气接口
def get_weather_for_bj():
    url = "https://api.seniverse.com/v3/weather/daily.json"
    parmas = {
        "key": "pulfwc1ujvotexij",
        "location": "beijing", "language": "zh-Hans", "unit": "c", "start": 0, "days": 5
    }
    r = requests.get(url, parmas)
    j = r.json()
    weather_str = "北京{date:%m-%d}天气预报:{w}，{temp1}℃~{temp2}℃".format(date=this_day+timedelta(days=2),
                                                                 w=j['results'][0]["daily"][1]['text_day'],
                                                                 temp1=j['results'][0]["daily"][1]['low'],
                                                                 temp2=j['results'][0]["daily"][1]['high'])
    return weather_str


def generate_report_for_column(x, hook):
    """
    """
    def get_x_data(y):
        v1 = df.loc["${DATE}"][y]
        v2 = diff_day.loc["${DATE}"][y]
        v3 = diff_week.loc["${DATE}"][y]
        v4 = this_week_wtd[y]
        v5 = last_week[y]
        return [v1, v2, v3, v4, v5]

    if x not in COMPUTE_COLS:
        value1 = get_x_data(x)
        print(value1)
        flag1 = "⬆️️" if value1[1] > 0 else "⬇️️"
        if hook in DETAILS_HOOK:
            text = "{flag1}「{x}」  {v1}, {value1[1]:.2%}, {value1[2]:.2%}; {v2}, {v3}".format(
                x=x[3:],
                flag1=flag1,
                value1=value1,
                v1=number_formatter_helper(value1[0]),
                v2=number_formatter_helper(value1[3]),
                v3=number_formatter_helper(value1[4]))
        else:
            text = "{flag1}「{x}」  {v1}, {value1[1]:.2%}, {value1[2]:.2%}".format(
                x=x[3:],
                flag1=flag1,
                value1=value1,
                v1=number_formatter_helper(value1[0]))
    else:  # 百分比指标
        v1 = df.loc["${DATE}"][x]
        v2 = diff_day_abs.loc["${DATE}"][x]
        v3 = diff_week_abs.loc["${DATE}"][x]
        flag = "⬆️️" if v2 > 0 else "⬇️️"
        text = "{flag}「{x}」  {v1:.3%}, {v2:.2f}pct, {v3:.2f}pct".format(
            x=x[3:], flag=flag, v1=v1, v2=v2 * 100, v3=v3 * 100)
    return text


for hook in WEBHOOKS:
    texts = ["{w}\n依次为指标环比走势、当日值、日环比、周同比；\n『抖→火』：".format(w=get_weather_for_bj())]
    for x in AW2HS:
        texts.append(generate_report_for_column(x[0], hook))
    texts.append("\n『火→抖』：")
    for x in HS2AW:
        texts.append(generate_report_for_column(x[0], hook))
    if DEBUG:
        if hook == DEBUG_WEBHOOK:
            post_to_lark("VCD内容侧数据播报 ${DATE} {wd:%a}".format(wd=this_day),
                         "\n".join(texts), hook)
    else:
        post_to_lark("VCD内容侧数据播报 ${DATE} {wd:%a}".format(wd=this_day),
                     "\n".join(texts), hook)

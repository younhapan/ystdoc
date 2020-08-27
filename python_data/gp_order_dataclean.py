# coding: utf-8
import pandas as pd
import xlrd
import openpyxl

df = pd.read_excel('/Users/younhapan/Downloads/imsi_decoded.xlsx')
df['imsi_decoded'] = df['imsi_decoded'].apply(lambda x: str(x))
df['country_code'] = df['imsi_decoded'].apply(lambda x: x[0:3])

def country_name(x):
	if x == '404' or x == '405' or x=='406':
		x='india'
	else:
		x='other'
	return x

df['country'] = df['country_code'].apply(country_name)
result = df.drop_duplicates(subset=['user_id','order_id','country'])

order_cnt = result.groupby('order_id')['country'].count()
order_cnt = order_cnt.reset_index()
order_cnt.rename(columns={'country':'order_num'}, inplace=True)

tmp = pd.merge(result,order_cnt, on='order_id')
final = tmp[(tmp['order_num']==1)|((tmp['order_num']==2)& (tmp['country']=='india'))]

repo7 = pd.read_csv('/Users/younhapan/Downloads/salesreport_201907.csv')
repo8 = pd.read_csv('/Users/younhapan/Downloads/salesreport_201908.csv')
repo9 = pd.read_csv('/Users/younhapan/Downloads/salesreport_201909.csv')
repo_all = pd.concat([repo7,repo8,repo9])

order_info = pd.merge(final, repo_all, left_on = 'order_id', right_on = 'Order Number', how = 'left')
order_info = order_info[order_info['app'] == 'funshare']
order_info = order_info[['order_time','user_id','order_id','app','sku_iap_id','price','cash_type','payment','account_id','imsi_decoded','country','Currency of Sale']]

pt = order_info.groupby(['country','Currency of Sale'])['user_id'].nunique()

with pd.ExcelWriter('/Users/younhapan/Downloads/issue_5109_v4.xlsx') as writer:
	order_info.to_excel(writer, sheet_name = 'raw_data', encoding = 'utf-8', index = False)
	pt.to_excel(writer, sheet_name = 'pivot_table', encoding = 'utf-8')
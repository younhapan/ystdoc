import pandas as pd
import xlrd
import openpyxl
origin_path='/Users/younhapan/Downloads/'
origin_name='6018'
df=pd.read_csv(origin_path+origin_name+'.csv',encoding='utf-8')
df.to_excel('/Users/younhapan/Downloads/'+origin_name+'.xlsx',index=False,encoding='utf-8')

# with open('/Users/younhapan/Downloads/yunxia.csv') as f:
# 	content = f.read()
# 	print(content)
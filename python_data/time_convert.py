import pandas as pd
from datetime import datetime, timezone, timedelta

df=pd.DataFrame()
df['play_time']=[1564719410,1564805076,1564938505,1564939337,1564939425,1564940576,1564940588,1565023404,1565100013,1565100071,1565123454,1565123481,1565123501,1565123531,1565123550,1565196866,1565196900,1565197667,1565197686,1565197702,1565197717]

middle_east_date=[]
bj_date=[]

for i in df['play_time']:
	t=datetime.utcfromtimestamp(i)
	utc_dt = t.replace(tzinfo=timezone.utc)
	middle_east_dt = utc_dt.astimezone(timezone(timedelta(hours=3)))
	# bj_dt = utc_dt.astimezone(timezone(timedelta(hours=8)))
	# print(middle_east_dt)

	middle_east_date.append(middle_east_dt.strftime('%Y%m%d %H:%M:%S'))
	print(middle_east_date)
	# bj_date.append(bj_dt.strftime('%Y%m%d %H:%M:%S'))

# df['middle_east_date']=pd.to_datetime(middle_east_date)
# df['bj_date']=pd.to_datetime(bj_date)
# print(df)

nested_lists = [[1, 2], [[3, 4], [5, 6], [[7, 8], [9, 10], [[11, [12, 13]]]]]]

flatten = lambda x: [y for l in x for y in flatten(l)] if type(x) is list else [x]

def flatten(x, result=[]):
    if type(x) is list:
        for l in x:
            flatten(l)
    else:
    	result.append(x)

    return result



def logit(logtext = 'this sentence'):
    def logging_decorator(func):
        @wraps(func)
        def wrapped_function(*args, **kwargs):
            log_string = func.__name__+' was called'
            print(logtext,log_string)
            return func(*args, **kwargs)
        return wrapped_function
    return logging_decorator
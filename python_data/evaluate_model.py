import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
df = pd.read_csv('/Users/younhapan/Downloads/give-me-some-credit-dataset/cs-training.csv')
data = df.iloc[:,1:12]

def set_missing(df):
    process_df = df.iloc[:,[5,0,1,2,3,4,6,7,8,9]]
    known = process_df[process_df.MonthlyIncome.notnull()].values
    unknown = process_df[process_df.MonthlyIncome.isnull()].values
    X = known[:,1:]
    y = known[:,0]
    rfr = RandomForestRegressor(random_state=0, n_estimators=200, max_depth=3, n_jobs=-1)
    rfr.fit(X,y)
    predicted = rfr.predict(unknown[:,1:]).round(0)
    print(predicted)
    df.loc[(df.MonthlyIncome.isnull()),'MonthlyIncome'] = predicted
    return df

data = set_missing(data)
data = data.dropna()
data = data.drop_duplicates()
# data.to_csv('/Users/younhapan/Downloads/MissingData.csv', encoding='utf-8', index=False)

data = data[data['age']>0]
data = data[data['NumberOfTime30-59DaysPastDueNotWorse']<90]
data = data[data['NumberOfTime60-89DaysPastDueNotWorse']<90]
data = data[data['NumberOfTimes90DaysLate']<90]
data['SeriousDlqin2yrs'] = 1-data['SeriousDlqin2yrs'] #正常履约支付利息客户=1，违约客户=0
data = data.reset_index(drop=True)

from sklearn.model_selection import train_test_split
Y = data['SeriousDlqin2yrs']
X = data.iloc[:,1:]
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.3, random_state=0)
train = pd.concat([Y_train, X_train], axis=1)
test = pd.concat([Y_test, X_test], axis=1)
classTest = test.groupby('SeriousDlqin2yrs')['SeriousDlqin2yrs'].count()
# train.to_csv('/Users/younhapan/Downloads/TrainData.csv', index=False)
# test.to_csv('/Users/younhapan/Downloads/TestData.csv', index=False)

# 自动分箱 计算变量iv、分箱区间、woe
from scipy import stats
def mono_bin(Y, X, n=20):
    r = 0
    good = Y.sum()
    bad = Y.count()-good

    # 最优分箱
    while np.abs(r) < 1:
        d1 = pd.DataFrame({'X':X, 'Y':Y, 'Bucket': pd.qcut(X, n, duplicates='drop')})
        d2 = d1.groupby('Bucket', as_index= True)
        r, p = stats.spearmanr(d2.mean().X, d2.mean().Y)
        n = n-1

    d3 = pd.DataFrame(d2.X.min(), columns = ['min'])
    d3['min'] = d2.min().X
    d3['max'] = d2.max().X
    d3['sum'] = d2.sum().Y
    d3['total'] = d2.count().Y
    d3['rate'] = d2.mean().Y
    d3['woe'] = np.log((d3['rate']/(1-d3['rate']))/(good/bad))

    d3['goodattribute']=d3['sum']/good
    d3['badattribute']=(d3['total']-d3['sum'])/bad
    iv = ((d3['goodattribute']-d3['badattribute'])*d3['woe']).sum()
    d4 = (d3.sort_values(by = 'min')).reset_index(drop = True)
    print("=" * 60)
    print(d4)
    cut = []
    cut.append(float('-inf'))
    for i in range(1,n+1):
        qua = X.quantile(i/(n+1))
        cut.append(round(qua,4))
    cut.append(float('inf'))
    woe = list(d4['woe'].round(3))
    return d4,iv,cut,woe

# 人工分箱 计算变量iv、分箱区间、woe
def self_bin(Y,X,cat):
    good = Y.sum()
    bad = Y.count()-good
    d1 = pd.DataFrame({'X':X,'Y':Y,'Bucket':pd.cut(X, cat, duplicates='drop')})
    d2 = d1.groupby(['Bucket'])
    d3 = pd.DataFrame(d2.X.min(),columns=['min'])
    d3['min'] = d2.X.min()
    d3['max'] = d2.X.max()
    d3['sum'] = d2.Y.sum()
    d3['total'] = d2.Y.count()
    d3['rate'] = d2.Y.mean()
    d3['goodattribute'] = d3['sum']/good
    d3['badattribute'] = (d3['total']-d3['sum'])/bad
    d3['woe'] = np.log(d3['goodattribute']/d3['badattribute'])

    iv = ((d3['goodattribute']-d3['badattribute'])*d3['woe']).sum()
    d4 = d3.sort_index(by='min')

    print(d4)
    print('='*60)
    woe = list(d3['woe'].values)
    return d4,iv,woe

cutx3 = [-float('inf'), 0, 1, 3, 5, float('inf')]
cutx6 = [-float('inf'), 1, 2, 3, 5, float('inf')]
cutx7 = [-float('inf'), 0, 1, 3, 5, float('inf')]
cutx8 = [-float('inf'), 0,1, 2, 3, float('inf')]
cutx9 = [-float('inf'), 0, 1, 3, float('inf')]
cutx10 = [-float('inf'), 0, 1, 2, 3, 5, float('inf')]

x1_d, ivx1, cutx1, woex1 = mono_bin(train['SeriousDlqin2yrs'], train['RevolvingUtilizationOfUnsecuredLines'], n=20)
x2_d, ivx2, cutx2, woex2 = mono_bin(train['SeriousDlqin2yrs'], train['age'], n=20)
x4_d, ivx4, cutx4, woex4 = mono_bin(train['SeriousDlqin2yrs'], train['DebtRatio'], n=20)
x5_d, ivx5, cutx5, woex5 = mono_bin(train['SeriousDlqin2yrs'], train['MonthlyIncome'], n=20)

x3_d, ivx3, woex3 = self_bin(train['SeriousDlqin2yrs'], train['NumberOfTime30-59DaysPastDueNotWorse'], cutx3)
x6_d, ivx6, woex6 = self_bin(train['SeriousDlqin2yrs'], train['NumberOfOpenCreditLinesAndLoans'], cutx6)
x7_d, ivx7, woex7 = self_bin(train['SeriousDlqin2yrs'], train['NumberOfTimes90DaysLate'], cutx7)
x8_d, ivx8, woex8 = self_bin(train['SeriousDlqin2yrs'], train['NumberRealEstateLoansOrLines'], cutx8)
x9_d, ivx9, woex9 = self_bin(train['SeriousDlqin2yrs'], train['NumberOfTime60-89DaysPastDueNotWorse'], cutx9)
x10_d, ivx10, woex10 = self_bin(train['SeriousDlqin2yrs'], train['NumberOfDependents'], cutx10)


# 计算各变量之间的相关性系数
import matplotlib.pyplot as plt
import seaborn as sns
corr = data.corr()
xticks = ['x0','x1','x2','x3','x4','x5','x6','x7','x8','x9','x10'] #x轴标签
yticks = list(corr.index) #y轴标签
fig = plt.figure()
ax1 = fig.add_subplot(1, 1, 1)
sns.heatmap(corr, annot=True, cmap='rainbow', ax=ax1, annot_kws={'size': 9, 'weight': 'bold', 'color': 'blue'}) #相关性系数热力图
ax1.set_xticklabels(xticks, rotation=0, fontsize=10)
ax1.set_yticklabels(yticks, rotation=0, fontsize=10)
plt.show()

# 各变量iv值
import matplotlib.pyplot as plt
ivlist=[ivx1,ivx2,ivx3,ivx4,ivx5,ivx6,ivx7,ivx8,ivx9,ivx10] # 各变量IV
index=['x1','x2','x3','x4','x5','x6','x7','x8','x9','x10'] # x轴的标签
fig1 = plt.figure(1)
ax1 = fig1.add_subplot(1, 1, 1)
x = np.arange(len(index))+1
ax1.bar(x, ivlist, width=0.4) # 生成柱状图
ax1.set_xticks(x)
ax1.set_xticklabels(index, rotation=0, fontsize=12)
ax1.set_ylabel('IV(Information Value)', fontsize=14) # 在柱状图上添加数字标签
for a, b in zip(x, ivlist):
    plt.text(a, b + 0.01, '%.4f' % b, ha='center', va='bottom', fontsize=10)
plt.show()


# 替换变量值为woe
def replace_woe(series, cut, woe):
    woelist = []
    i = 0
    while i < len(series):
        value = series[i]
        j = len(cut) - 2 # 排除正负无穷大
        m = len(cut) - 2
        while j>=0:
            if value>=cut[j]:
                j-=1
            else:
                j-=1
                m-=1
        i+=1
        woelist.append(woe[m])
    return woelist

data['RevolvingUtilizationOfUnsecuredLines_woe'] = pd.Series(replace_woe(data['RevolvingUtilizationOfUnsecuredLines'], cutx1, woex1))
data['age_woe'] = pd.Series(replace_woe(data['age'], cutx2, woex2))
data['NumberOfTime30-59DaysPastDueNotWorse_woe'] = pd.Series(replace_woe(data['NumberOfTime30-59DaysPastDueNotWorse'], cutx3, woex3))
data['DebtRatio_woe'] = pd.Series(replace_woe(data['DebtRatio'], cutx4, woex4))
data['MonthlyIncome_woe'] = pd.Series(replace_woe(data['MonthlyIncome'], cutx5, woex5))
data['NumberOfOpenCreditLinesAndLoans_woe'] = pd.Series(replace_woe(data['NumberOfOpenCreditLinesAndLoans'], cutx6, woex6))
data['NumberOfTimes90DaysLate_woe'] = pd.Series(replace_woe(data['NumberOfTimes90DaysLate'], cutx7, woex7))
data['NumberRealEstateLoansOrLines_woe'] = pd.Series(replace_woe(data['NumberRealEstateLoansOrLines'], cutx8, woex8))
data['NumberOfTime60-89DaysPastDueNotWorse_woe'] = pd.Series(replace_woe(data['NumberOfTime60-89DaysPastDueNotWorse'], cutx9, woex9))
data['NumberOfDependents_woe'] = pd.Series(replace_woe(data['NumberOfDependents'], cutx10, woex10))
# data.to_csv('WoeData.csv', index=False)


# logistic regression
# 因变量
import statsmodels.api as sm
Y = data['SeriousDlqin2yrs'].values
# 自变量，剔除对因变量影响不明显的变量
X = data[['RevolvingUtilizationOfUnsecuredLines_woe', 'age_woe','NumberOfTime30-59DaysPastDueNotWorse_woe','NumberOfTimes90DaysLate_woe', 'NumberOfTime60-89DaysPastDueNotWorse_woe']]
X = X.values
X1 = sm.add_constant(X)
logit = sm.Logit(Y,X1)
result = logit.fit()
print(result.summary())
print(np.exp(result.params))


# 检验logistic
# 因变量
from sklearn import metrics
Y_test = test['SeriousDlqin2yrs']
# 自变量，剔除对因变量影响不明显的变量，与模型变量对应
X_test = test.drop(['SeriousDlqin2yrs', 'DebtRatio', 'MonthlyIncome', 'NumberOfOpenCreditLinesAndLoans','NumberRealEstateLoansOrLines', 'NumberOfDependents'], axis=1)
X3 = sm.add_constant(X_test)
resu = result.predict(X3)
# 进行预测
fpr, tpr, threshold = metrics.roc_curve(Y_test, resu)
rocauc = metrics.auc(fpr, tpr)
# 计算AUC
plt.plot(fpr, tpr, 'b', label='AUC = %0.2f' % rocauc)
# 生成ROC曲线
plt.legend(loc='lower right')
plt.plot([0, 1], [0, 1], 'r--')
plt.xlim([0, 1])
plt.ylim([0, 1])
plt.ylabel('真正率')
plt.xlabel('假正率')
plt.show()


X_test['queue_success_cnt_woe'] = pd.Series(replace_woe(X_test['queue_success_cnt'], cutqueue_success_cnt, woequeue_success_cnt))
X_test['stay_dur_woe'] = pd.Series(replace_woe(X_test['stay_dur'], cutstay_dur, woestay_dur))
X_test['send_cnt_woe'] = pd.Series(replace_woe(X_test['send_cnt'], cutsend_cnt, woesend_cnt))
X_test['receive_cnt_woe'] = pd.Series(replace_woe(X_test['receive_cnt'], cutreceive_cnt, woereceive_cnt))











import pandas as pd

shopping_list = [['豆奶','莴苣'],
                 ['莴苣','尿布','葡萄酒','甜菜'],
                 ['豆奶','尿布','葡萄酒','橙汁'],
                 ['莴苣','豆奶','尿布','葡萄酒'],
                 ['莴苣','豆奶','尿布','橙汁']]

shopping_df=pd.DataFrame(shopping_list)

#dataframe数据转换为源数据df_arr
# way1
df_arr = shopping_df.stack().groupby(level=0).apply(list).tolist()
# way2
# def deal(data):
# 	return data.dropna().tolist()

# df_arr = shopping_df.apply(deal,axis=1).tolist()

# 将数据源df_arr转化为模型可接受数据
from mlxtend.preprocessing import TransactionEncoder
te = TransactionEncoder()
df_tf = te.fit_transform(df_arr)
df = pd.DataFrame(df_tf,columns=te.columns_)

# 频繁项集
from mlxtend.frequent_patterns import apriori
frequent_itemsets = apriori(df,min_support=0.05,use_colnames=True)
frequent_itemsets.sort_values(by='support',ascending=False,inplace=True)

#print(frequent_itemsets)
#print(frequent_itemsets[frequent_itemsets.itemsets.apply(lambda x:len(x)>1)])

# 关联规则
from mlxtend.frequent_patterns import association_rules
association_rule = association_rules(frequent_itemsets,metric='confidence',min_threshold=0.9)
association_rule.sort_values(by='leverage',ascending=False,inplace=True)
print(association_rule)
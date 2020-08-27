import xlrd
import matplotlib.pyplot as plt
import numpy as np
from sklearn import model_selection
from sklearn.linear_model import LogisticRegression
from sklearn import metrics


data = xlrd.open_workbook('gua.xlsx')
sheet = data.sheet_by_index(0)
Density = sheet.col_values(6) # 密度
Sugar = sheet.col_values(7) # 含糖率
Res = sheet.col_values(8) # 分类

# 读取原始数据
X = np.array([Density, Sugar])
# y的尺寸为(17,)
y = np.array(Res)
X = X.reshape(17,2)

# 绘制分类数据
f1 = plt.figure(1)
plt.title('watermelon_3a')
plt.xlabel('density')
plt.ylabel('ratio_sugar')

# 绘制散点图（x轴为密度，y轴为含糖率）
plt.scatter(X[y == 0,0], X[y == 0,1], marker = 'o', color = 'k', s=100, label = 'bad')
plt.scatter(X[y == 1,0], X[y == 1,1], marker = 'o', color = 'g', s=100, label = 'good')
plt.legend(loc = 'upper right')
plt.show()

# 从原始数据中选取一半数据进行训练，另一半数据进行测试
X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.5, random_state=0)

# 逻辑回归模型
log_model = LogisticRegression()
# 训练逻辑回归模型
log_model.fit(X_train, y_train)

# 预测y的值
y_pred = log_model.predict(X_test)

# 查看测试结果
print(metrics.confusion_matrix(y_test, y_pred))
print(metrics.classification_report(y_test, y_pred))
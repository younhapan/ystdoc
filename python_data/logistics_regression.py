#logistics regression
import numpy as np
import pandas as pd

#确定原始数据的维度
train_m=train_x_orgin.shape[0]
test_m=test_x_orgin.shape[0]

#对原始数据进行向量化(扁平化处理)
train_x_flatten=train_x_orgin.reshape(train_x_orgin.shape[0],-1).T
test_x_flatten=test_x_orgin.reshape(test_x_orgin.shape[0],-1).T

#对向量进行标准化处理(将所有数据进行正态分布处理,RGB通道值=255）
train_x_standard=train_x_flatten/255
test_x_flatten=test_x_flatten/255

#定义sigmoid函数
def sigmoid(z):
	a=1/(1+np.exp(-z))
	return a

#参数初始化initialization，初始化参数w和b(w是一个列向量，b是一个数值)，参数初始值全部设置为0
def initialize_with_zeros(w,b):
	w=np.zeros((dim,1))
	b=0
	return w,b

#定义传播函数propagate，求解dJ/dw、学习率α(采用下降梯度方向),正向传播+反向传播
def propagate(w,b,X,Y):
	"""
	传参：
	w: shape(num_point*num_point*3,1),3个通道内的像素值
	b: 1个标量
	X: shape(num_points*num_points*3,m)
	Y: shape(1,m) 行向量
	m: X.shape[1]，样本点个数m

	返回值：
	cost，dw，db
	"""

	# 前向传播，cost函数
	# 激活函数：tanh和sigmoid
	A=sigmoid((np.dot(w.T,X))+b)
	cost= -(np.sum(Y*np.log(A)+(1-Y)*np.log(1-A)))/m

	#反向传播，求dw，db
	dZ=A-Y
	dw=(np.dot(X,dZ.T))/m
	db=(np.sum(dZ))/m

	#返回值：
	grads={"dw":dw,
			"db":db}

	return grads,cost

# 优化函数optimization(确定下一次迭代更新的w,b)
def optimization(w,b,X,Y,num_iterations,learning_rate,print_cost=True):
	costs=[]

	for i in range(num_iterations):
		grads,cost=propagate(w,b,X,Y)
		dw=grads["dw"]
		db=grads["db"]

		w-=learning_rate*dw
		b-=learning_rate*db

		if i%100==0:
			costs.append(cost)

		if print_cost and i%100==0:
			print("cost after iteration %i: %f" %(i,cost))


	params={"w":w,
			"b":b}
	grads={"dw":dw,
			"db":db}

	return costs,params,grads

# 预测函数predict
def predict(w,b,X):
	m=X.shape[1]
	Y_prediction=np.zeros((1,m))

	A=sigmoid(np.dot(w.T,X)+b)
	for i in range(m):
		if A[0,i]>0.5:
			Y_prediction[0,i]=1
		else:
			Y_prediction[0,i]=0

	return Y_prediction


def logistic_model(X_train,Y_train,X_test,Y_test,learning_rate=0.1,num_iterations=2000,print_cost=False):
    # 获特征维度，初始化参数：
    dim = X_train.shape[0]
    W,b = initialize_with_zeros(dim)

    # 梯度下降，迭代求出模型参数：
    params,grads,costs = optimize(W,b,X_train,Y_train,num_iterations,learning_rate,print_cost)
    W = params['w']
    b = params['b']

    # 用学得的参数进行预测：
    prediction_train = predict(W,b,X_test)
    prediction_test = predict(W,b,X_train)

    # 计算准确率，分别在训练集和测试集上：
    accuracy_train = 1 - np.mean(np.abs(prediction_train - Y_train))
    accuracy_test = 1 - np.mean(np.abs(prediction_test - Y_test))
    print("Accuracy on train set:",accuracy_train )
    print("Accuracy on test set:",accuracy_test )

   # 分析和检查，得到的所有参数、超参数存进字典d返回
    d = {"costs": costs,
         "Y_prediction_test": prediction_test ,
         "Y_prediction_train" : prediction_train ,
         "w" : W,
         "b" : b,
         "learning_rate" : learning_rate,
         "num_iterations": num_iterations,
         "train_acy":train_acy,
         "test_acy":test_acy
        }
    return d




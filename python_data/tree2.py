#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import operator

def loadDataSet():
    """
    导入数据
    @ return dataSet: 读取的数据集
    """
    # 对数据进行处理
    dataSet = pd.read_csv('isFish.csv', delimiter=',')
    # dataSet = dataSet.replace('yes', 1).replace('no', 0)
    labelSet = list(dataSet.columns.values)
    dataSet = dataSet.values
    return dataSet, labelSet

# def calculate_EntD(dataset, n, m):
#     # 计算以某点划分的信息熵：
#     # 计算信息熵时，先将原连续数据改为字符串形式（<=m, >m）的离散数据
#     dataset_new = copy.deepcopy(dataset)
#     for i in range(len(dataset_new)):
#         a = str(m)
#         if float(dataset_new[i][n]) <= m:
#             dataset_new[i][n] = '<='+a
#         else:
#             dataset_new[i][n] = '>'+a
#     #这里计算信息熵的函数，直接使用决策树中的相关函数
#     entd = EntD(dataset_new)
#     count = statistics(dataset_new, n)
#     entdv = EntDv(count)
#     gaind = GainD(entd, entdv, len(dataset_new))
#     #返回信息熵
#     return gaind

def calcShannonEnt(dataSet):
    """
    计算给定数据集的信息熵（香农熵）
    @ param dataSet: 数据集
    @ return shannonEnt: 香农熵
    """
    numEntries = len(dataSet)
    labelCounts = {} # 当前样本每个类型的数量
    for featVec in dataSet:
        # 当前样本类型
        currentLabel = featVec[-1]
        # 如果当前类别不在labelCounts里面，则创建
        if currentLabel not in labelCounts.keys():
            labelCounts[currentLabel] = 0
        labelCounts[currentLabel] += 1
    shannonEnt = 0.0
    for key in labelCounts:
        prob = float(labelCounts[key]) / numEntries
        shannonEnt -= prob*np.log2(prob)
    return shannonEnt

def splitDataSet(dataSet, axis, value):
    """
    划分数据集, 提取所有满足一个特征的值, 得到去除该特征的数据集
    @ param dataSet: 数据集
    @ param axis: 划分数据集的特征
    @ param value: 提取出来满足某特征的list
    """
    retDataSet = []
    for featVec in dataSet:
        # 将相同数据特征的提取出来
        if featVec[axis] == value:
            reducedFeatVec = list(featVec[:axis])
            reducedFeatVec.extend(featVec[axis+1:])
            retDataSet.append(reducedFeatVec)
    return retDataSet

def chooseBestFeature(dataSet):
    """
    选择最优的划分属性
    @ param dataSet: 数据集
    @ return bestFeature: 最佳划分属性
    """
    numFeature = len(dataSet[0])-1 # 属性的个数
    baseEntroy = calcShannonEnt(dataSet)
    bestInfoGain = 0.0
    bestFeature = -1
    for i in range(numFeature):
        featureList = [example[i] for example in dataSet] # 获取第i个特征所有可能的取值
        uniqueVals = set(featureList) # 第i个特征去除重复值后的取值
        newEntropy = 0.0
        for value in uniqueVals:
            subDataSet = splitDataSet(dataSet, i, value) # 特征为i的数据集，特征i取value的数据集
            prob = len(subDataSet) / float(len(dataSet)) # 特征为i的数据集取value时占总数的比例
            newEntropy += prob * calcShannonEnt(subDataSet)
        inforGain = baseEntroy - newEntropy # 求信息增益

        if inforGain > bestInfoGain:
            bestInfoGain = inforGain
            bestFeature = i
    return bestFeature

def majorityCnt(classList):
    """
    递归构建决策树
    @ param classList: 类别列表
    @ return sortedClassCount[0][0]: 当遍历完所有特征时，决策节点返回出现次数最多的类别
    """
    classCount = {}
    for vote in classList:
        if vote not in classCount.keys():
            classCount[vote] = 0
        classCount[vote] += 1
    # 排序
    sortedClassCount = sorted(classCount.items(), key=operator.itemgetter(1), reverse=True)
    # 返回出现次数最多的
    return sortedClassCount[0][0]

def createTree(dataSet, labels):
    """
    构造决策树
    @ param dataSet: 数据集
    @ param labels: 标签集
    @ return myTree: 决策树
    """
    classList = [example[-1] for example in dataSet]
    # 当类别与属性完全相同时停止(所有数据都属于同一类别势，即返回该类别)
    if classList.count(classList[0]) == len(classList):
        return classList[0]
    # 遍历完所有特征值时，返回数量最多的
    if (len(dataSet[0]) == 1):
        return majorityCnt(classList)

    # 获取最佳划分属性
    bestFeat = chooseBestFeature(dataSet)
    bestFeatLabel = labels[bestFeat]
    myTree = {bestFeatLabel:{}}
    del(labels[bestFeat]) # 清空labels[bestFeat]，删除已选择的特征
    featValues = [example[bestFeat] for example in dataSet]
    uniqueVals = set(featValues)
    for value in uniqueVals:
        subLabels = labels[:] #子节点
        myTree[bestFeatLabel][value] = createTree(splitDataSet(dataSet, bestFeat, value), subLabels) # 递归调用创建决策树
    return myTree

def classfiy(myTree,labels,test):
    firstStr = list(myTree.keys())[0]       #需要获取首个特征的列号，以便从测试数据中取值比较
    secondDict = myTree[firstStr]           #获得第二个字典
    featIndex = labels.index(firstStr)      #获取测试集对应特征数值
    for key in secondDict.keys():
        if(test[featIndex] == key):
            if(type(secondDict[key]).__name__ == 'dict'):       #判断该值是否还是字典，如果是，则继续递归
                classlabel = classfiy(secondDict[key],labels,test)
            else:
                classlabel = secondDict[key]
    return classlabel

if __name__ == '__main__':
    dataSet, labelSet = loadDataSet()
    shannonEnt = calcShannonEnt(dataSet)
    tree= createTree(dataSet, labelSet)
    print (tree)

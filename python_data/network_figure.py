
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import codecs
import networkx as nx
import matplotlib.pyplot as plt


""" 第一步:读取数据并获取姓名 """
data = pd.read_csv("data.csv",encoding ="gb2312") #中文乱码
print data[:4]
print data[u'姓名'] #获取某一列数据
print type(data[u'姓名'])
name = []
for n in data[u'姓名']:
    name.append(n)
print name[0]


""" 第二步:计算共现矩阵 定义函数实现 """
a = np.zeros([2,3])
print a
print len(name)
word_vector = np.zeros([len(name),len(name)]) #共现矩阵

#1.计算学院共线矩阵
i = 0
while i<len(name):  #len(name)
    academy1 = data[u'学院'][i]
    j = i + 1
    while j<len(name):
        academy2 = data[u'学院'][j]
        if academy1==academy2: #学院相同
            word_vector[i][j] += 1
            word_vector[j][i] += 1
        j = j + 1
    i = i + 1
print word_vector
np_data = np.array(word_vector)  #矩阵写入文件
pd_data = pd.DataFrame(np_data)
pd_data.to_csv('result.csv')
#2.计算大数据金融班级共线矩阵
#3.计算性别共线矩阵
#4.计算宿舍楼层共线矩阵
"""
i = 0
while i<len(name):  #len(name)
    academy1 = data[u'宿舍楼层'][i]
    j = i + 1
    while j<len(name):
        academy2 = data[u'宿舍楼层'][j]
        if academy1==academy2: #相同
            word_vector[i][j] += 1
            word_vector[j][i] += 1
        j = j + 1
    i = i + 1
print word_vector
"""


""" 第三步:共现矩阵计算(学生1 学生2 共现词频)文件 """
words = codecs.open("word_node.txt", "a+", "utf-8")
i = 0
while i<len(name):  #len(name)
    student1 = name[i]
    j = i + 1
    while j<len(name):
        student2 = name[j]
        #判断学生是否共现 共现词频不为0则加入
        if word_vector[i][j]>0:
            words.write(student1 + " " + student2 + " "
                + str(word_vector[i][j]) + "\r\n")
        j = j + 1
    i = i + 1
words.close()


""" 第四步:图形生成 """
a = []
f = codecs.open('word_node.txt','r','utf-8')
line = f.readline()
print line
i = 0
A = []
B = []
while line!="":
    a.append(line.split())   #保存文件是以空格分离的
    print a[i][0],a[i][1]
    A.append(a[i][0])
    B.append(a[i][1])
    i = i + 1
    line = f.readline()
elem_dic = tuple(zip(A,B))
print type(elem_dic)
print list(elem_dic)
f.close()

import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family']='sans-serif'

colors = ["red","green","blue","yellow","purple"]
G = nx.Graph()
G.add_edges_from(list(elem_dic))
pos=nx.spring_layout(G,iterations=50)
#nx.draw(G,with_labels=True,pos=nx.random_layout(G),font_size=12,node_size=2000,node_color=colors) #alpha=0.3
#
pos=nx.random_layout(G)
nx.draw_networkx_nodes(G, pos, alpha=0.2,node_size=1200,node_color=colors)
nx.draw_networkx_edges(G, pos, node_color='r', alpha=0.3) #style='dashed'
nx.draw_networkx_labels(G, pos, font_family='sans-serif', alpha=0.5) #font_size=5
plt.show










import community
import pandas as pd
import networkx as nx

G_karate = nx.karate_club_graph()

c_degree = nx.degree_centrality(G_karate) #度中心度
c_eigenvector = nx.eigenvector_centrality(G_karate) #特征向量中心度






# Plot the centrality of the nodes
plt.figure(figsize=(18, 12))# Degree Centrality
f, axarr = plt.subplots(2, 2, num=1)
plt.sca(axarr[0,0])
nx.draw(G_karate, cmap = plt.get_cmap('inferno'), node_color = c_degree, node_size=300, pos=pos, with_labels=True)
axarr[0,0].set_title('Degree Centrality', size=16)# Eigenvalue Centrality
plt.sca(axarr[0,1])
nx.draw(G_karate, cmap = plt.get_cmap('inferno'), node_color = c_eigenvector, node_size=300, pos=pos, with_labels=True)
axarr[0,1].set_title('Eigenvalue Centrality', size=16)# Proximity Centrality
plt.sca(axarr[1,0])
nx.draw(G_karate, cmap = plt.get_cmap('inferno'), node_color = c_closeness, node_size=300, pos=pos, with_labels=True)
axarr[1,0].set_title('Proximity Centrality', size=16)# Betweenness Centrality
plt.sca(axarr[1,1])
nx.draw(G_karate, cmap = plt.get_cmap('inferno'), node_color = c_betweenness, node_size=300, pos=pos, with_labels=True)
axarr[1,1].set_title('Betweenness Centrality', size=16)

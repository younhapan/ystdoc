# -*- coding: utf-8 -*-

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

"""
读数据
"""
data = pd.read_csv('/Users/younhapan/Downloads/togo_0916_0922.csv')
data = data[data['reward_coin_cnt'] > 1000]
d1 = data['rewarder_id']
d2 = data['receiver_id']
d3 = data['reward_coin_cnt']


"""
生成网络图点、边、权重
"""
z = zip(d1,d2,1/d3)
z1 = list(z)
f1 = nx.Graph()
f1.add_weighted_edges_from(z1)

"""
计算最短距离，生成距离矩阵，进行分层聚类
"""
from sklearn.cluster import AgglomerativeClustering

len_dis = dict(nx.all_pairs_dijkstra_path_length(f1))
all_node = list(f1.nodes())

n = nx.number_of_nodes(f1)
dis = [100 for i in range(n*n)]
distances = pd.DataFrame(np.array(dis).reshape((n,n)), index = all_node, columns = all_node) #初始化距离矩阵

for s in all_node:
    for e in len_dis[s].keys():
        distances.loc[s,e]=len_dis[s][e]

# clustering = AgglomerativeClustering(n_clusters = 2,linkage = 'average',affinity = 'precomputed').fit_predict(distances)

"""
设置网络图画图参数
"""

# figure_pos
pos = nx.spring_layout(f1,iterations = 450)
plt.figure(figsize = (15,15), frameon = True, edgecolor = None)

# node_size
sizes_value = [10 for i in range(n)]
node_sizes = dict(zip(all_node,sizes_value))

reward_cnt = pd.DataFrame(data.groupby('rewarder_id')['reward_coin_cnt'].sum())
receive_cnt = pd.DataFrame(data.groupby('receiver_id')['reward_coin_cnt'].sum())

reward_sort = reward_cnt.sort_values('reward_coin_cnt',ascending = False)
receive_sort = receive_cnt.sort_values('reward_coin_cnt',ascending = False)

top5_rewarder = reward_sort[0:5].index.tolist()
top5_receiver = receive_sort[0:5].index.tolist()
top5_node = list(set(top5_rewarder + top5_receiver))

top6_rewarder = reward_sort[6:20].index.tolist()
top6_receiver = receive_sort[6:20].index.tolist()
top6_node = list(set(top6_rewarder + top6_receiver))

top_rewarder = reward_sort[0:20].index.tolist()
top_receiver = receive_sort[0:20].index.tolist()
top_node = list(set(top_rewarder + top_receiver))

for i in top5_node:
    node_sizes[i] = 500

for j in list(set(top6_node).difference(set(top5_node))):
    node_sizes[j] = 100

# node_label
labels = dict()
rewarder=data[['rewarder_id','rewarder_gender']]
receiver=data[['receiver_id','receiver_gender']]
rewarder.columns=['user_id','gender']
receiver.columns=['user_id','gender']
user_country=pd.concat([rewarder,receiver]).drop_duplicates().set_index(['user_id'])
for i in top_node:
    labels[i] = str(i) + ' ' + user_country.loc[i,'gender']

# node_color
colors=['teal' for i in range(n)]
color = dict(zip(all_node,colors))

both_top5 = list(set(top5_rewarder).intersection(set(top5_receiver)))
only_top5_rewarder = list(set(top5_rewarder).difference(set(top5_receiver)))
only_top5_receiver = list(set(top5_receiver).difference(set(top5_rewarder)))
for i in both_top5:
    color[i] = 'purple'

for i in only_top5_rewarder:
    color[i] = 'red'

for i in only_top5_receiver:
    color[i] = 'green'

both_top6 = list(set(top6_rewarder).intersection(set(top6_receiver)).difference(set(top5_node)))
only_top6_rewarder = list(set(top6_rewarder).difference(set(top6_receiver)).difference(set(top5_node)))
only_top6_receiver = list(set(top6_receiver).difference(set(top6_rewarder)).difference(set(top5_node)))
for i in both_top6:
    color[i] = 'purple'

for i in only_top6_rewarder:
    color[i] = 'red'

for i in only_top6_receiver:
    color[i] = 'green'

nx.draw_networkx_nodes(f1, pos, alpha = 0.5, node_size = list(node_sizes.values()), node_color = list(color.values()), label = 'User')
nx.draw_networkx_edges(f1, pos, alpha = 0.5, label = 'Relation')
nx.draw_networkx_labels(f1, pos, labels = labels, font_weight = 'bold', font_size = 8)
plt.title('chatroom_network')
plt.legend(loc = 'best',framealpha = 0.5)

plt.savefig('/Users/younhapan/Downloads/togo_0916_0922.png')
plt.show()
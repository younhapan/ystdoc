import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np

"""
读数据
"""
data=pd.read_csv('/Users/younhapan/Downloads/net.csv')
data=data[data['reward_coin_cnt']>100]
d1=data['rewarder_id']
d2=data['receiver_id']
d3=data['reward_coin_cnt']

"""
生成网络图点、边、权重
"""
z=zip(d1,d2,1/d3)
z1=list(z)
f1=nx.Graph()
f1.add_weighted_edges_from(z1)

"""
计算最短距离，生成距离矩阵，进行分层聚类
"""
from sklearn.cluster import AgglomerativeClustering

len_dis=dict(nx.all_pairs_dijkstra_path_length(f1))
all_node=list(f1.nodes())
n=nx.number_of_nodes(f1)
distances = pd.DataFrame(np.ones((n,n)), index = all_node, columns=all_node)
for s in all_node:
    for e in len_dis[s].keys():
        distances.loc[s,e]=len_dis[s][e]

clustering = AgglomerativeClustering(n_clusters=4,linkage='average',affinity='precomputed').fit_predict(distances)

"""
设置网络图参数
"""
pos=nx.spring_layout(f1,iterations=400)
plt.figure(figsize=(10,10), frameon=True, edgecolor=None)

colors='rbcg'
color=[colors[clustering[i]] for i in range(len(clustering))]

sizes_value=[50 for i in range(n)]
node_sizes=dict(zip(all_node,sizes_value))
reward_cnt=data.groupby('rewarder_id')['reward_coin_cnt'].sum()
receive_cnt=data.groupby('receiver_id')['reward_coin_cnt'].sum()
top_rewarder=pd.DataFrame(reward_cnt)['reward_coin_cnt'].idxmax()
top_receiver=pd.DataFrame(receive_cnt)['reward_coin_cnt'].idxmax() #argmax使用idxmax代替
node_sizes[top_rewarder]=500
node_sizes[top_receiver]=500

nx.draw_networkx_nodes(f1, pos, alpha=0.8, node_size = list(node_sizes.values()), node_color = color, label='User')
nx.draw_networkx_edges(f1, pos, alpha=0.5, label='Relation')
plt.title('chatroom_network')
plt.legend(loc='best',framealpha=0.5)
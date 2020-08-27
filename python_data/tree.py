from math import log
import operator

def create_data():#构建数据和标签
    dataSet=[['short', 'long hair', 'thin', 'female'],
             ['high', 'short hair', 'thin', 'male'],
             ['short', 'long hair', 'fat', 'female'],
             ['high', 'long hair', 'thin', 'female'],
             ['short', 'short hair', 'fat', 'male'],
             ['short', 'short hair', 'thin', 'female'],
             ['high', 'short hair', 'fat', 'male'],
             ['high', 'long hair', 'fat', 'male'],
             ['short', 'short hair', 'thin', 'male'],
             ['high', 'short hair', 'thin', 'female'],
             ['short', 'long hair', 'fat', 'female']]
    labels= ['stature', 'hair', 'weight', 'gender']
    return dataSet, labels

def cal_entropy(dataSet):
    num=len(dataSet)
    label_count={}
    for fea in dataSet:
        current_label=fea[-1]#统计每条数据的类,数据最后一个元素是所属类别
        if current_label not in label_count.keys():
            label_count[current_label]=0
        label_count[current_label]+=1 #计算每个类中有多少数据
    entropy=0.0
    for i in label_count:#计算经验熵
        Pi=float(label_count[i])/num
        entropy-=Pi*log(Pi,2)
    return entropy

def remove_feature(dataSet,axis,feature):#去除某个特征
    retdataset=[]
    for featVec in dataSet:
        if featVec[axis]==feature:
            reducedata=featVec[:axis]#某个特征前数据
            reducedata.extend(featVec[axis+1:])#某个特征后数据
            #去掉了axis
            retdataset.append(reducedata)
    return retdataset

def choose_best_feature(dataSet):
    entropy=cal_entropy(dataSet)
    feature_num=len(dataSet[0])-1 # 标签数量
    max_mutual_info=0
    best_feature=-1
    for i in range(feature_num):
        feature_list=[example[i] for example in dataSet]
        feature_class=set(feature_list) # 得到该特征i的所有可能取值
        conditional_entropy=0
        for value in feature_class:
            retdataset=remove_feature(dataSet, i, value)
            Pi=len(retdataset)/float(len(dataSet))
            conditional_entropy+=Pi*cal_entropy(retdataset) #求条件熵
        mutual_info=entropy-conditional_entropy #信息增益
        if (mutual_info>max_mutual_info):
            max_mutual_info=mutual_info
            best_feature=i
    return best_feature

def majority_vote(class_list):
    class_count={}
    for vote in class_list:
        if vote not in class_count.keys():
            class_count[vote]=0
        class_count[vote]+=1
    sort_class_count=sorted(class_count.items(),key=operator.itemgetter(1),reverse=True)
    #排序来决定该节点的类
    return sort_class_count[0][0]


def create_tree(dataSet,labels):
    class_list=[example[-1] for example in dataSet]
    if class_list.count(class_list[0])==len(class_list):
        return class_list[0]
    if len(dataSet[0])==1:
        #节点已没有特征可以继续分解
        return majority_vote(class_list)

    best_feature=choose_best_feature(dataSet)
    best_feature_label=labels[best_feature]
    my_tree={best_feature_label:{}}
    del(labels[best_feature])
    #删掉已选择的特征
    feature=[example[best_feature] for example in dataSet]
    feature_class=set(feature)
    for value in feature_class:
        sublabels=labels[:]
        my_tree[best_feature_label][value]=create_tree(remove_feature(dataSet,best_feature,value), sublabels)
        #迭代生成决策树
    return my_tree


if __name__ == '__main__':
    dataSet,labels=create_data()
    print(create_tree(dataSet,labels))

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import jieba
import pandas as pd

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer

# jieba.cut(s)                精确模式，返回一个可迭代的数据类型
# jieba.cut(s, cut_all=True)  全模式，输出文本s中所有可能单词
# jieba.cut_for_search(s)     搜索引擎模式，适合搜索引擎建立索引的分词结果
# jieba.lcut(s)               精确模式，返回一个列表类型，建议使用
# jieba.lcut(s, cut_all=True) 全模式，返回一个列表类型，建议使用
# jieba.lcut_for_search(s)    搜索引擎模式，返回一个列表类型，建议使用
# jieba.add_word(w)           向分词词典中添加新词w

# 基于TF-IDF
# jieba.analyse.extract_tags(sentence, topK=20, withWeight=False, allowPOS=())
# sentence：待提取的文本
# topK：返回几个TF/IDF权重最大的关键字，默认20
# withWeight：是否一并返回关键词权重值，默认值为False
# allowPOS：仅包括指定词性的词，默认值为空，即不筛选；可选项('ns','n','vn','v')

# 基于TextRank
#jieba.analyse.textrank(sentence, topK=20, withWeight=False, allowPOS=('ns','n','vn','v')) 

# 词性标注
# jieba.posseg.POSTokenizer(tokenizer=None) 
# 新建自定义分词器，tokenizer 参数可指定内部使用的 jieba.Tokenizer 分词器。jieba.posseg.dt 为默认词性标注分词器。

# 词性对照表
# 名词 (1个一类，7个二类，5个三类) 
    # 名词分为以下子类： 
    # n 名词 
    # nr 人名 
    # nr1 汉语姓氏 
    # nr2 汉语名字 
    # nrj 日语人名 
    # nrf 音译人名 
    # ns 地名 
    # nsf 音译地名 
    # nt 机构团体名 
    # nz 其它专名 
    # nl 名词性惯用语 
    # ng 名词性语素
# 时间词(1个一类，1个二类) 
    # t 时间词 
    # tg 时间词性语素
# 处所词(1个一类) 
    # s 处所词
# 方位词(1个一类) 
    # f 方位词
# 动词(1个一类，9个二类) 
    # v 动词 
    # vd 副动词 
    # vn 名动词 
    # vshi 动词“是” 
    # vyou 动词“有” 
    # vf 趋向动词 
    # vx 形式动词 
    # vi 不及物动词（内动词） 
    # vl 动词性惯用语 
    # vg 动词性语素
# 形容词(1个一类，4个二类) 
    # a 形容词 
    # ad 副形词 
    # an 名形词 
    # ag 形容词性语素 
    # al 形容词性惯用语
# 区别词(1个一类，2个二类) 
    # b 区别词 
    # bl 区别词性惯用语
# 状态词(1个一类) 
    # z 状态词
# 代词(1个一类，4个二类，6个三类) 
    # r 代词 
    # rr 人称代词 
    # rz 指示代词 
    # rzt 时间指示代词 
    # rzs 处所指示代词 
    # rzv 谓词性指示代词 
    # ry 疑问代词 
    # ryt 时间疑问代词 
    # rys 处所疑问代词 
    # ryv 谓词性疑问代词 
    # rg 代词性语素
# 数词(1个一类，1个二类) 
    # m 数词 
    # mq 数量词
# 量词(1个一类，2个二类) 
    # q 量词 
    # qv 动量词 
    # qt 时量词
# 副词(1个一类) 
    # d 副词
# 介词(1个一类，2个二类) 
    # p 介词 
    # pba 介词“把” 
    # pbei 介词“被”
# 连词(1个一类，1个二类) 
    # c 连词 
    # cc 并列连词
# 助词(1个一类，15个二类) 
    # u 助词 
    # uzhe 着 
    # ule 了 喽 
    # uguo 过 
    # ude1 的 底 
    # ude2 地 
    # ude3 得 
    # usuo 所 
    # udeng 等 等等 云云 
    # uyy 一样 一般 似的 般 
    # udh 的话 
    # uls 来讲 来说 而言 说来 
    # uzhi 之 
    # ulian 连 （“连小学生都会”）
# 叹词(1个一类) 
    # e 叹词
# 语气词(1个一类) 
    # y 语气词(delete yg)
# 拟声词(1个一类) 
    # o 拟声词
# 前缀(1个一类) 
    # h 前缀
# 后缀(1个一类) 
    # k 后缀
# 字符串(1个一类，2个二类) 
    # x 字符串 
    # xx 非语素字 
    # xu 网址URL
# 标点符号(1个一类，16个二类) 
    # w 标点符号 
    # wkz 左括号，全角：（ 〔 ［ ｛ 《 【 〖 〈 半角：( [ { < 
    # wky 右括号，全角：） 〕 ］ ｝ 》 】 〗 〉 半角： ) ] { > 
    # wyz 左引号，全角：“ ‘ 『 
    # wyy 右引号，全角：” ’ 』 
    # wj 句号，全角：。 
    # ww 问号，全角：？ 半角：? 
    # wt 叹号，全角：！ 半角：! 
    # wd 逗号，全角：， 半角：, 
    # wf 分号，全角：； 半角： ; 
    # wn 顿号，全角：、 
    # wm 冒号，全角：： 半角： : 
    # ws 省略号，全角：…… … 
    # wp 破折号，全角：—— －－ ——－ 半角：— —- 
    # wb 百分号千分号，全角：％ ‰ 半角：% 
    # wh 单位符号，全角：￥ ＄ ￡ ° ℃ 半角：$

def readTXT(filepath):
    res = []
    with open(filepath, 'r') as f:
        for line in f:
            res.append(line.replace('\n',''))
    return res

def stopWordsFilter(words_lst, stop_words):
    res = []
    for word in words_lst:
        if word not in stop_words and word != '\t':
            res.append(word.lower())
    return res

def getTopK(words, weight, k):
    res = pd.DataFrame(columns = ['keyWords'])
    df = pd.DataFrame(weight, columns = words)
    for idx in df.index:
        topWords = df.loc[idx,:].sort_values(ascending=False)[:k].index.tolist()
        tfidf = df.loc[idx, topWords].values.tolist()
        res.loc[idx, 'keyWords'] = list(zip(topWords, tfidf))
    return res

if __name__ == '__main__':
    df = pd.read_csv('./data/传奇种子app.csv')[['pkg_name','app_name','desc','crawl_category_origin','category_code']]
    df['words_lst'] = df['desc'].apply(lambda x: jieba.lcut(x))

    # 初始化停用词
    stop_words = []
    for file in os.listdir('./stop_words'):
        if file != '.DS_Store':
            stop_words.extend(readTXT('./stop_words/%s'%file))

    stop_words = list(set(stop_words))

    # 分词过滤停用词
    df['words_lst_cleaned'] = df['words_lst'].apply(lambda x: stopWordsFilter(x, stop_words))

    words_lst = list(map(lambda x: ' '.join(x), df.words_lst_cleaned))

    vectorizer = CountVectorizer()
    words_arr = vectorizer.fit_transform(words_lst).toarray()
    words =  vectorizer.get_feature_names()
    # X.toarray()

    transformer = TfidfTransformer()
    weight = transformer.fit_transform(words_arr).toarray()

    # 获取每个APP的头部10个关键字
    keyWords = getTopK(words, weight, 10)
    keyWords.to_excel('./data/传奇种子app关键字提取.xlsx', encoding='utf-8')
    # 单机、分解、专精、贪玩、屠龙、蓝月、半兽人、传奇、爆装、情怀

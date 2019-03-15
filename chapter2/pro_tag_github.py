import pymysql
import itertools

MINSUPPORTPCT = 5   #最小支持阈值
allSingletonTags = []
allDoubletonTags = set()
doubletonSet = set()


db = pymysql.connect(
                        host='localhost',
                        user='root',
                        passwd='...',
                        db='fc_project_tags',
                        port=3306,
                        charset='latin1')
cursor = db.cursor()

# baskets :　篮子总个数
queryBaskets = "select count(distinct project_id) from fc_project_tags;"
cursor.execute(queryBaskets)
baskets = cursor.fetchone()[0]  #46510


# minsupport : 篮子的最小数量 
# 即，篮子总数的　5% =  46510 * 5% = 2325.5
minsupport = baskets * (MINSUPPORTPCT /100)

# tagNameQuery ：　查找符合最小支持阈值的标签
# 最小支持阈值：即上一步的结果, 2325.5
# 即，有那些标签/商品是至少被2325.5 个篮子包含的
tagNameQuery = "select distinct tag_name \
                from fc_project_tags \
                group by 1 \
                having count(project_id) >= %s \
                order by tag_name"
cursor.execute(tagNameQuery,(minsupport))
singletons = cursor.fetchall()

for singleton in singletons:
    allSingletonTags.append(singleton[0])


# ------------------------------------------------------

def findDoubletons():
    # 1. use the list of allSingletonTags to make the doubleton candidates
    doubletonCandidates = list(itertools.combinations(allSingletonTags, 2))
            # allSingletonTags : 所有满足最小支持阈值的产品
            # doubletonCandidates : 将这些产品两两组合，不重复

    # 2. figure out if this doubleton candidate is frequent
    for (index, candidate) in enumerate(doubletonCandidates):
        tag1 = candidate[0] # 产品1
        tag2 = candidate[1] # 产品2

        # getDoubletonFrequencyQuery : 产品1和产品2同时出现在同一个篮子的个数
        getDoubletonFrequencyQuery = "select count(fpt1.project_id) \
                                     FROM fc_project_tags fpt1 \
                                     INNER JOIN fc_project_tags fpt2 \
                                     ON fpt1.project_id = fpt2.project_id \
                                     WHERE fpt1.tag_name = %s \
                                     AND fpt2.tag_name = %s"
        cursor.execute(getDoubletonFrequencyQuery, (tag1, tag2))
        count = cursor.fetchone()[0]

        # add frequent doubleton to database                
        if count > minsupport:
            print (tag1,tag2,"[",count,"]")
            
            insertPairQuery = "insert INTO fc_project_tags_pairs \
                                (tag1, tag2, num_projs) \
                                VALUES (%s,%s,%s);"
            cursor.execute(insertPairQuery,(tag1, tag2, count))
            db.commit()

            
            # save the frequent doubleton(两两组合) to our final list
            doubletonSet.add(candidate)         
            # add terms(单个标签/产品) to a set of all doubleton terms 
            allDoubletonTags.add(tag1)
            allDoubletonTags.add(tag2)
  
def findTripletons():
    # 1. allDoubletonTags --> tripleton candidates
    tripletonCandidates = list(itertools.combinations(allDoubletonTags,3))

    # 2. sort each candidate tuple 
    # tripletonCandidatesSorted : 给每一个三元组内部按首字母排序, 
                                # = tripletonCandidates
                                # 所有的三元组
    tripletonCandidatesSorted = []
    for tc in tripletonCandidates:
        tripletonCandidatesSorted.append(sorted(tc))
    
    # 3. figure out if this tripleton candidate is frequent
    #    all doubletons inside this tripleton candidate MUST also be frequent
    for (index, candidate) in enumerate(tripletonCandidatesSorted):   
        # a. 由单个三元组构造其内部的二元组
        doubletonsInsideTripleton = list(itertools.combinations(candidate,2))
        
        # b.　判断各个二元组是否在doubletonSet中，满足闭包属性
        tripletonCandidateRejected = 0
        for (index, doubleton) in enumerate(doubletonsInsideTripleton):
            if doubleton not in doubletonSet:
                tripletonCandidateRejected = 1
                break

        # c. set up queries
        #       1) count this tripleton's number
        getTripletonFrequencyQuery = "select count(fpt1.project_id) \
                                        FROM fc_project_tags fpt1 \
                                        INNER JOIN fc_project_tags fpt2 \
                                        ON fpt1.project_id = fpt2.project_id \
                                        INNER JOIN fc_project_tags fpt3 \
                                        ON fpt2.project_id = fpt3.project_id \
                                        WHERE (fpt1.tag_name = %s \
                                        AND fpt2.tag_name = %s \
                                        AND fpt3.tag_name = %s)"
        #       2) if count > minsupport, insert this tripleton
        insertTripletonQuery = "insert into fc_project_tag_triples \
                                (tag1, tag2, tag3, num_projs) \
                                VALUES (%s,%s,%s,%s)"
       
        # d. 对满足闭包属性的三元组进行统计 
        if tripletonCandidateRejected == 0:
            cursor.execute(getTripletonFrequencyQuery, (candidate[0],
                                                        candidate[1],
                                                        candidate[2]))
            count = cursor.fetchone()[0]
            # e. 将　frequent的三元组插入database
            if count > minsupport:
                print (candidate[0],",",
                       candidate[1],",",
                       candidate[2],
                       "[",count,"]")
                cursor.execute(insertTripletonQuery,
                                (candidate[0],
                                 candidate[1],
                                 candidate[2],
                                 count))
                db.commit()

def generateRules():
    # pull final list of tripletons to make the rules
    getFinalListQuery = "select tag1, tag2, tag3, num_projs \
                   FROM fc_project_tag_triples"
    cursor.execute(getFinalListQuery)
    triples = cursor.fetchall()
    for(triple) in triples:
        tag1 = triple[0]
        tag2 = triple[1]
        tag3 = triple[2]
        ruleSupport = triple[3]
        
        # 每个三元组可能生成 3 条规则
        calcSCAV(tag1, tag2, tag3, ruleSupport)
        calcSCAV(tag1, tag3, tag2, ruleSupport)
        calcSCAV(tag2, tag3, tag1, ruleSupport)
        # print("*")

def calcSCAV(tagA, tagB, tagC, ruleSupport):
    # 1. Support
    #       =　三元组所在的篮子数　/ 篮子总数
    ruleSupportPct = round((ruleSupport/baskets),2)

    # 2. Confidence    
    #       = 三元组所在的篮子数　/ 二元组篮子数
    queryConf = "select num_projs \
              FROM fc_project_tags_pairs \
              WHERE (tag1 = %s AND tag2 = %s) \
              OR    (tag2 = %s AND tag1 = %s)"
    cursor.execute(queryConf, (tagA, tagB, tagA, tagB))
    pairSupport = cursor.fetchone()[0]
        # pairSupport：tagA,tagB　为二元组时的篮子数
    confidence = round((ruleSupport / pairSupport),2)
    
    # 3. Added Value
    queryAV = "select count(*) \
              FROM fc_project_tags \
              WHERE tag_name= %s"
    cursor.execute(queryAV, tagC)
    supportTagC = cursor.fetchone()[0]
        # supportTagC : tagC 所有篮子总数
    supportTagCPct = supportTagC/baskets
        # supportTagCPct : tagC 的支持度
    addedValue = round((confidence - supportTagCPct),2)
        # addedValue : 附加值得分，　 = 整条规则的置信度　-　右侧项支持度
    
    # Result
    print(tagA,",",tagB,"->",tagC,
          "[S=",ruleSupportPct,
          ", C=",confidence,
          ", AV=",addedValue,
          "]")
        
def compareTwoTags(X,Y):

    # grab basic counts from the database 
    queryBaskets = "select count(distinct project_id) from fc_project_tags;"
    cursor.execute(queryBaskets)
    baskets = cursor.fetchone()[0]  #46510

    # X  and Y 的篮子数
    supportForXYQuery = "SELECT count(*) FROM fc_project_tags WHERE tag_name=%s" 
    cursor.execute(supportForXYQuery, (X))
    supportForX = cursor.fetchone()[0]

    cursor.execute(supportForXYQuery, (Y))
    supportForY = cursor.fetchone()[0]

    # (X,Y) 的篮子数
    pairSupportQuery = "SELECT num_projs FROM fc_project_tags_pairs WHERE tag1 = %s AND tag2 = %s"
    cursor.execute(pairSupportQuery, (X, Y))
    pairSupport = cursor.fetchone()[0]
     
    # calculate support : support of pair, divided by num baskets
    pairSupportAsPct = pairSupport / baskets

    # calculate confidence of X->Y
    # confidence(X->Y) = P(Y|X) 
    supportForXAsPct = supportForX / baskets
    confidenceXY = pairSupportAsPct / supportForXAsPct

    # calculate confidence of Y->X
    supportForYAsPct = supportForY / baskets
    confidenceYX = pairSupportAsPct/ supportForYAsPct

    # calculate added value X->Y
    AVXY = confidenceXY - supportForYAsPct
    AVYX = confidenceYX - supportForXAsPct

    print("Support for ",X,"U",Y,":", round(pairSupportAsPct, 4))
    print("Conf.",X,"->",Y,":", round(confidenceXY, 4))
    print("Conf.",Y,"->",X,":", round(confidenceYX, 4))
    print("AV",X,"->",Y,":", round(AVXY, 4))
    print("AV",Y,"->",X,":", round(AVYX, 4))

# ----------------------------------------------------------
# findDoubletons()
# findTripletons()
# generateRules()

X = 'Internet'
Y = 'Web'
compareTwoTags(X,Y)



cursor.close()
db.close()
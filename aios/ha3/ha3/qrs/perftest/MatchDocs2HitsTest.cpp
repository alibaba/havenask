#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Request.h>
#include <ha3/common/MatchDocs.h>
#include <map>
#include <autil/mem_pool/Pool.h>
#include <autil/TimeUtility.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/SortClause.h>
#include <ha3/common/ConfigClause.h>
#include <ha3/common/Hits.h>
#include <ha3/common/MatchDocs2Hits.h>
#include <ha3/common/SortDescription.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <autil/Thread.h>
#include <vector>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(queryparser);
BEGIN_HA3_NAMESPACE(qrs);

class MatchDocs2HitsTest : public TESTBASE {
public:
    MatchDocs2HitsTest();
    ~MatchDocs2HitsTest();
public:
    void setUp();
    void tearDown();
protected:
    void runProcess(int repeat);
    void prepareRequest(common::RequestPtr &requestPtr);
    common::MatchDocs* prepareMatchDocs(uint32_t matchDocNum,
            std::map<std::string, int> attributeMap);
protected:
    autil::mem_pool::Pool _pool;
    common::RequestPtr _requestPtr;
    common::MatchDocs *_matchDocs;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, MatchDocs2HitsTest);


MatchDocs2HitsTest::MatchDocs2HitsTest() { 
}

MatchDocs2HitsTest::~MatchDocs2HitsTest() { 
}

void MatchDocs2HitsTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void MatchDocs2HitsTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

MatchDocs* MatchDocs2HitsTest::prepareMatchDocs(uint32_t matchDocNum, map<string, int> attributeMap) {
    MatchDocs *matchDocs = new MatchDocs();
    matchDocs->setTotalMatchDocs(matchDocNum);
    matchDocs->setHasPrimaryKeyFlag(true);

    MatchDocAllocator *matchDocAllocator = new MatchDocAllocator(&_pool);    

    //declare
    map<string, int>::const_iterator iter = attributeMap.begin();
    vector<VariableReference<int32_t>*> intRefVec;
    vector<VariableReference<float>*> floatRefVec;
    vector<VariableReference<string>*> strRefVec;
    for (; iter != attributeMap.end(); ++iter) {
        string name = "";
        if ("int" == iter->first) {
            for (int i = 0; i < iter->second; ++i) {
                name = "int" + autil::StringUtil::toString(i);
                VariableReference<int32_t>* ref = matchDocAllocator->declareVariable<int32_t>(name, false, true);
                intRefVec.push_back(ref);
            }
        } else if ("float" == iter->first) {
            for (int i = 0; i < iter->second; ++i) {
                name = "float" + autil::StringUtil::toString(i);
                VariableReference<float>* ref = matchDocAllocator->declareVariable<float>(name, false, true);
                floatRefVec.push_back(ref);
            }
        } else if ("string" == iter->first) {
            for (int i = 0; i < iter->second; ++i) {
                name = "string" + autil::StringUtil::toString(i);
                VariableReference<string>* ref =
                    matchDocAllocator->declareVariable<string>(name, true, true);
                strRefVec.push_back(ref);
            }
        }
    }

    for (uint32_t i = 0; i < matchDocNum; ++i) {
        MatchDoc *matchDoc = matchDocAllocator->allocateMatchDoc(i);
        ASSERT_TRUE(matchDoc);
        matchDocs->addMatchDoc(matchDoc);

        for (size_t i = 0; i < intRefVec.size(); ++i) {
            intRefVec[i]->set(matchDoc->getVariableSlab(), 32);
        }
        for (size_t i = 0; i < floatRefVec.size(); ++i) {
            floatRefVec[i]->set(matchDoc->getVariableSlab(), 32.5f);
        }
        for (size_t i = 0; i < strRefVec.size(); ++i) {
            strRefVec[i]->set(matchDoc->getVariableSlab(), string("abcd"));
        }
    }
    
    VSAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setVSAllocator(matchDocAllocatorPtr);
    return matchDocs;
}

void MatchDocs2HitsTest::prepareRequest(RequestPtr &requestPtr) {
    SortClause *sortClause = new SortClause;
    SortDescription *sortDescription = new SortDescription("int0");
    SyntaxExpr* sortExpr = new AtomicSyntaxExpr("int0",
            vt_int32, ATTRIBUTE_NAME);
    sortDescription->setRootSyntaxExpr(sortExpr);
    sortDescription->setRankFlag(false);
    sortClause->addSortDescription(sortDescription);
    requestPtr->setSortClause(sortClause);

    ConfigClause *configClause = new ConfigClause;
    configClause->setStartOffset(0);
    requestPtr->setConfigClause(configClause);
}

TEST_F(MatchDocs2HitsTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    map<string, int> attributeMap;
    uint32_t matchDocNum;
    _requestPtr.reset(new Request);
    prepareRequest(_requestPtr);
    
    matchDocNum = 1000;
    attributeMap["int"] = 4;
    attributeMap["float"] = 4;
    attributeMap["string"] = 2;
    _matchDocs = prepareMatchDocs(matchDocNum, attributeMap);
    ASSERT_TRUE(_matchDocs);
    ASSERT_EQ(matchDocNum, _matchDocs->size());

    vector<ThreadPtr> vt;
    static const int THREAD_NUM = 8;
    static const int repeatTimes = 1000;

    int64_t beginTime = TimeUtility::currentTime();
    for (int i = 0; i < THREAD_NUM; ++i) {
        ThreadPtr threadPtr = Thread::createThread(std::tr1::bind(&MatchDocs2HitsTest::runProcess, this, repeatTimes));
        vt.push_back(threadPtr);
    }
    for (size_t i = 0; i < vt.size(); ++i) {
        vt[i]->join();
    }
    int64_t endTime = TimeUtility::currentTime();
    double qps =  1000000.0 / (endTime - beginTime) * THREAD_NUM * repeatTimes; 
    HA3_LOG(ERROR, "\n***********qps : %.1lf", qps);
}

void MatchDocs2HitsTest::runProcess(int repeat) {
    for (int i = 0; i < repeat; ++i) {
        MatchDocs2Hits convert;
        Hits *hits = convert.convert(_matchDocs, _requestPtr);
        delete hits;
    }
}

END_HA3_NAMESPACE(qrs);


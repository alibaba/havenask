#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/CacheMinScoreFilter.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/SortExpressionCreator.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <ha3/rank/ComparatorCreator.h>
#include <ha3/rank/MultiHitCollector.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class CacheMinScoreFilterTest : public TESTBASE {
public:
    CacheMinScoreFilterTest();
    ~CacheMinScoreFilterTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestFilterByMinScore(
            const std::string &sortFlagStr,
            const std::string &minScores,
            const std::string &matchDocs,
            const std::string &result,
            bool isScored,
            size_t expectCount = 0);
    void innerTestSelectExtraMatchDocs(
            size_t selectCount, 
            const std::string &matchDocStr,
            const std::string &sortFlagStr, 
            const std::string &expectFilteredDocs, 
            const std::string &expectDocs);
    autil::mem_pool::PoolVector<matchdoc::MatchDoc> createMatchDocs(
            const std::string &matchDocsStr, 
            const std::vector<AttributeExpression*> &fakeExprs);
    void createFakeAndSortExprs(
            std::vector<AttributeExpression*> &fakeExprs,
            SortExpressionVector &sortExprs,
            const std::string &sortFlagStr);
protected:
    common::Ha3MatchDocAllocatorPtr _allocatorPtr;
    SortExpressionCreator *_sortExprCreator;
    autil::mem_pool::Pool *_pool;    
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, CacheMinScoreFilterTest);


CacheMinScoreFilterTest::CacheMinScoreFilterTest() {
    _pool = new mem_pool::Pool(1024);    
    _sortExprCreator = new SortExpressionCreator(NULL, NULL, 
            NULL, common::MultiErrorResultPtr(), _pool);
}

CacheMinScoreFilterTest::~CacheMinScoreFilterTest() {
    _allocatorPtr.reset();
    delete _sortExprCreator;
    delete _pool;
}

void CacheMinScoreFilterTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _allocatorPtr.reset(new common::Ha3MatchDocAllocator(_pool));
}

void CacheMinScoreFilterTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(CacheMinScoreFilterTest, testStoreMinScore) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeRankExpression *fakeAttrExpr = 
        new FakeRankExpression("fake_ref");
    fakeAttrExpr->allocate(_allocatorPtr.get());
    ComparatorCreator creator(_pool, false);
    SortExpression *sortExpr = 
        _sortExprCreator->createSortExpression(fakeAttrExpr, false);
    SortExpressionVector sortExprVec;
    sortExprVec.push_back(sortExpr);
    ComboComparator *cmp = creator.createSortComparator(sortExprVec);
    HitCollector hitCollector(1, _pool, _allocatorPtr, fakeAttrExpr, cmp);
    matchdoc::MatchDoc matchDoc = _allocatorPtr->allocate(0);
    matchdoc::Reference<double> *ref = fakeAttrExpr->getReference();
    ref->set(matchDoc, 0.1);
    hitCollector.collect(matchDoc);
    hitCollector.flush();
    CacheMinScoreFilter filter;
    filter.storeMinScore(&hitCollector, sortExprVec);
    ASSERT_EQ(size_t(1), filter._scores.size());
    ASSERT_DOUBLE_EQ(score_t(0.1), filter._scores[0]);
    delete fakeAttrExpr;
}

TEST_F(CacheMinScoreFilterTest, testStoreMinScoreMultiCollector) {
    HA3_LOG(DEBUG, "Begin Test!");
    vector<double> values;
    values.push_back(0.1);
    values.push_back(10);
    values.push_back(5);
    vector<AttributeExpression*> fakeExprs;
    for (size_t i = 0; i < values.size() + 1; ++i) {
        AttributeExpression *fakeAttrExpr = new FakeRankExpression("fake_ref");
        fakeExprs.push_back(fakeAttrExpr);
        fakeAttrExpr->allocate(_allocatorPtr.get());
    }
    
    ComboAttributeExpression *attrForHitCollector = new ComboAttributeExpression();
    attrForHitCollector->setExpressionVector(fakeExprs);
    MultiHitCollector multiHitCollector(_pool, _allocatorPtr,
            attrForHitCollector);
    
    SortExpressionVector exprs;
    for (size_t i = 0; i < values.size() + 1; ++i) {
        ComparatorCreator creator(_pool, false);
        SortExpression *sortExpr = 
            _sortExprCreator->createSortExpression(fakeExprs[i], false);
        SortExpressionVector sortExprVec;
        sortExprVec.push_back(sortExpr);
        exprs.push_back(sortExpr);                
        ComboComparator *cmp = creator.createSortComparator(sortExprVec);
        HitCollector *hitCollector = POOL_NEW_CLASS(_pool, HitCollector, 1,
                _pool, _allocatorPtr, NULL, cmp);
        multiHitCollector.addHitCollector(hitCollector);
        if (i == 2) {
            hitCollector->enableLazyScore(10);
        }
    }
    const std::vector<HitCollectorBase*> &hitCollectors =
        multiHitCollector.getHitCollectors();
    for (size_t i = 0; i < values.size(); ++i) {
        matchdoc::MatchDoc matchDoc = _allocatorPtr->allocate(i);
        matchdoc::Reference<double> *ref =
            ((FakeRankExpression*)fakeExprs[i])->getReference();
        ref->set(matchDoc, values[i]);
        hitCollectors[i]->collect(matchDoc);
        hitCollectors[i]->flush();
    }
    CacheMinScoreFilter filter;
    filter.storeMinScore(&multiHitCollector, exprs);
    ASSERT_EQ(size_t(4), filter._scores.size());
    ASSERT_DOUBLE_EQ(score_t(0.1), filter._scores[0]);
    ASSERT_DOUBLE_EQ(score_t(10), filter._scores[1]);
    ASSERT_EQ(numeric_limits<double>::min(), filter._scores[2]);
    ASSERT_EQ(numeric_limits<double>::min(), filter._scores[3]);
    for (size_t i = 0; i < fakeExprs.size(); ++i) {
        delete fakeExprs[i];
    }
    delete attrForHitCollector;
}

TEST_F(CacheMinScoreFilterTest, testFilterByMinScore) {
    string sortFlagStr = "false";
    string minScore = "4";
    string matchDocs = "1:1;"
                       "2:4;"
                       "3:4;"
                       "4:5;"
                       "5:2;";
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "2,3,4", true);
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "1,2,3,4,5", false);
}

TEST_F(CacheMinScoreFilterTest, testFilterByMinScoreMulti) {
    string sortFlagStr = "false, false, false";
    string minScore = "4,5,6";
    string matchDocs = "1:1,2,5;"
                       "2:4,4,4;"
                       "3:4,5,6;"
                       "4:5,6,7;"
                       "5:2,6,6;";
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "2,3,4,5", true);
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "1,2,3,4,5", false);
}

TEST_F(CacheMinScoreFilterTest, testFilterByMinScoreSortFlag) {
    string sortFlagStr = "true";
    string minScore = "4";
    string matchDocs = "1:1;"
                       "2:4;"
                       "3:4;"
                       "4:5;"
                       "5:2;";
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "1,2,3,5", true);
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "1,2,3,4,5", false);
}

TEST_F(CacheMinScoreFilterTest, testFilterByMinScoreMultiSortFlag) {
    string sortFlagStr = "true, false, true";
    string minScore = "4,-5,6";
    string matchDocs = "1:1,-2,5;"
                       "2:5,-6,10;"
                       "3:4,0,6;"
                       "4:10,-20,7;"
                       "5:-2,6,6;";
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "1,3,5", true);
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "1,2,3,4,5", false);
}

TEST_F(CacheMinScoreFilterTest, testFilterByMinScoreWithExpectCount_1) {
    string sortFlagStr = "false";
    string minScore = "4";
    string matchDocs = "1:1;"
                       "2:4;"
                       "3:4;"
                       "4:5;"
                       "5:2;";
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "2,3,4", true, 1);
}

TEST_F(CacheMinScoreFilterTest, testFilterByMinScoreWithExpectCount_2) {
    string sortFlagStr = "false";
    string minScore = "4";
    string matchDocs = "1:1;"
                       "2:4;"
                       "3:4;"
                       "4:5;"
                       "5:2;";
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "2,3,4", true, 3);
}

TEST_F(CacheMinScoreFilterTest, testFilterByMinScoreWithExpectCount_3) {
    HA3_LOG(DEBUG, "Begin Test!");
    string sortFlagStr = "false";
    string minScore = "4";
    string matchDocs = "1:1;"
                       "2:4;"
                       "3:4;"
                       "4:5;"
                       "5:2;";
    innerTestFilterByMinScore(sortFlagStr, minScore, matchDocs, "2,3,4,5", true, 4);
    
}

TEST_F(CacheMinScoreFilterTest, testSelectExtraMatchDocs_1) {
    HA3_LOG(DEBUG, "Begin Test!");
    string sortFlagStr = "true";
    string matchDocStr = "1:1;2:6;3:2;4:3;5:7;6:4;7:5;";
    innerTestSelectExtraMatchDocs(3, matchDocStr, sortFlagStr, 
                                  "1,3,4,6", "2,5,7");
}

TEST_F(CacheMinScoreFilterTest, testSelectExtraMatchDocs_2) {
    HA3_LOG(DEBUG, "Begin Test!");
    string sortFlagStr = "true";
    string matchDocStr = "1:1;2:6;3:2;4:3;5:7;6:4;7:5;";
    innerTestSelectExtraMatchDocs(0, matchDocStr, sortFlagStr, 
                                  "1,2,3,4,5,6,7", "");
}

TEST_F(CacheMinScoreFilterTest, testSelectExtraMatchDocs_3) {
    HA3_LOG(DEBUG, "Begin Test!");
    string sortFlagStr = "true";
    string matchDocStr = "1:1;2:6;3:2;4:3;5:7;6:4;7:5;";
    innerTestSelectExtraMatchDocs(7, matchDocStr, sortFlagStr, 
                                  "", "1,2,3,4,5,6,7");
}

TEST_F(CacheMinScoreFilterTest, testSelectExtraMatchDocsWithSortFlag) {
    HA3_LOG(DEBUG, "Begin Test!");
    string sortFlagStr = "false";
    string matchDocStr = "1:1;2:6;3:2;4:3;5:7;6:4;7:5;";
    innerTestSelectExtraMatchDocs(3, matchDocStr, sortFlagStr, 
                                  "2,5,6,7", "1,3,4");
}

TEST_F(CacheMinScoreFilterTest, testGetRankScore) {
    HA3_LOG(DEBUG, "Begin Test!");

    ASSERT_DOUBLE_EQ((score_t)0, CacheMinScoreFilter::getRankScore(NULL, matchdoc::INVALID_MATCHDOC));
    
#define TEST_WITH_TYPE(type) {                                          \
        matchdoc::Reference<type>* scoreRef = NULL;                     \
        common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(new common::Ha3MatchDocAllocator(_pool)); \
        scoreRef = matchDocAllocatorPtr->declare<type>(#type);          \
        auto matchDoc = matchDocAllocatorPtr->allocate(1);              \
        scoreRef->set(matchDoc, (type)1);                               \
        score_t score = CacheMinScoreFilter::getRankScore(scoreRef, matchDoc); \
        ASSERT_DOUBLE_EQ((score_t)1, score);                            \
        matchDocAllocatorPtr->deallocate(matchDoc);                     \
    }
    
    TEST_WITH_TYPE(int8_t);
    TEST_WITH_TYPE(int16_t);
    TEST_WITH_TYPE(int32_t);
    TEST_WITH_TYPE(int64_t);
    TEST_WITH_TYPE(uint8_t);
    TEST_WITH_TYPE(uint16_t);
    TEST_WITH_TYPE(uint32_t);
    TEST_WITH_TYPE(uint64_t);
    TEST_WITH_TYPE(float);
    TEST_WITH_TYPE(double);

#undef TEST_WITH_TYPE
        
}

void CacheMinScoreFilterTest::innerTestSelectExtraMatchDocs(
        size_t selectCount, 
        const string &matchDocStr,
        const string &sortFlagStr, 
        const string &expectFilteredDocs, const string &expectDocs)
{
    vector<AttributeExpression*> fakeExprs;
    SortExpressionVector sortExprs;
    createFakeAndSortExprs(fakeExprs, sortExprs, sortFlagStr);

    PoolVector<matchdoc::MatchDoc> docs = createMatchDocs(matchDocStr, fakeExprs);
    HA3_LOG(DEBUG, "**** doccount = %zd", docs.size());
    vector<matchdoc::MatchDoc> filteredMatchDocs;
    filteredMatchDocs.insert(filteredMatchDocs.end(), 
                             docs.begin(), docs.end());

    PoolVector<matchdoc::MatchDoc> matchDocs(_pool);

    ComparatorCreator compCreator(_pool);
    ComboComparator *comp = compCreator.createSortComparator(sortExprs);
    ASSERT_TRUE(comp);

    CacheMinScoreFilter filter;
    filter.selectExtraMatchDocs(selectCount, comp, filteredMatchDocs, matchDocs);
    
    StringTokenizer st1(expectFilteredDocs, ",", 
                        StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ((size_t)st1.getNumTokens(), filteredMatchDocs.size());
    StringTokenizer st2(expectDocs, ",", 
                        StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ((size_t)st2.getNumTokens(), matchDocs.size());

    for (size_t i = 0; i < filteredMatchDocs.size(); i++) {
        _allocatorPtr->deallocate(filteredMatchDocs[i]);
    }
    for (size_t i = 0; i < matchDocs.size(); i++) {
        _allocatorPtr->deallocate(matchDocs[i]);
    }
    for (size_t i = 0; i < fakeExprs.size(); ++i) {
        POOL_DELETE_CLASS(fakeExprs[i]);
    }
    POOL_DELETE_CLASS(comp);
}

void CacheMinScoreFilterTest::innerTestFilterByMinScore(
        const string &sortFlagStr, const string &minScores,
        const string &matchDocsStr, const string &result, bool isScored,
        size_t expectCount)
{
    _allocatorPtr.reset(new common::Ha3MatchDocAllocator(_pool));
    CacheMinScoreFilter filter;
    StringTokenizer st1(minScores, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st1.getNumTokens(); ++i) {
        filter._scores.push_back(StringUtil::fromString<double>(st1[i]));
    }
    vector<AttributeExpression*> fakeExprs;
    SortExpressionVector exprs;
    createFakeAndSortExprs(fakeExprs, exprs, sortFlagStr);
    
    PoolVector<matchdoc::MatchDoc> matchDocs = createMatchDocs(matchDocsStr, fakeExprs);
    
    ComboAttributeExpression *attrForHitCollector = new ComboAttributeExpression();
    attrForHitCollector->setExpressionVector(fakeExprs);
    MultiHitCollector hitCollector(_pool, _allocatorPtr, attrForHitCollector);
    if (!isScored) {
        hitCollector.enableLazyScore(10);
    }
    ComparatorCreator compCreator(_pool);
    ComboComparator *comp = compCreator.createSortComparator(exprs);
    HitCollector *singleCollector = POOL_NEW_CLASS(_pool, HitCollector, 1,
            _pool, _allocatorPtr, NULL, comp);
    hitCollector.addHitCollector(singleCollector);
    filter.filterByMinScore(&hitCollector, exprs, matchDocs, expectCount);
    StringTokenizer st3(result, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
    ASSERT_EQ(st3.getNumTokens(), matchDocs.size());
    for (size_t i = 0; i < st3.getNumTokens(); ++i) {
        docid_t docId = StringUtil::fromString<docid_t>(st3[i]);
        ASSERT_EQ(docId, matchDocs[i].getDocId());
    }
    for (size_t i = 0; i < matchDocs.size(); ++i) {
        _allocatorPtr->deallocate(matchDocs[i]);
    }
    for (size_t i = 0; i < fakeExprs.size(); ++i) {
        POOL_DELETE_CLASS(fakeExprs[i]);
    }
    delete attrForHitCollector;
}

PoolVector<matchdoc::MatchDoc> CacheMinScoreFilterTest::createMatchDocs(
        const string &matchDocsStr, const vector<AttributeExpression*> &fakeExprs)
{
    StringTokenizer st2(matchDocsStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                        | StringTokenizer::TOKEN_TRIM);
    PoolVector<matchdoc::MatchDoc> matchDocs(_pool);
    for (size_t i = 0; i < st2.getNumTokens(); ++i) {
        string matchDocStr = st2[i];
        pos_t pos = matchDocStr.find(':');
        docid_t docId = StringUtil::fromString<docid_t>(matchDocStr.substr(0, pos));
        StringTokenizer st3(matchDocStr.substr(pos + 1), ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                            | StringTokenizer::TOKEN_TRIM);
        assert(st3.getNumTokens() == fakeExprs.size());
        matchdoc::MatchDoc matchDoc = _allocatorPtr->allocate(docId);
        for (size_t j = 0; j < st3.getNumTokens(); ++j) {
            matchdoc::Reference<double> *ref =
                ((FakeRankExpression*)fakeExprs[j])->getReference();
            ref->set(matchDoc, StringUtil::fromString<double>(st3[j]));
        }
        matchDocs.push_back(matchDoc);
    }
    return matchDocs;
}    

void CacheMinScoreFilterTest::createFakeAndSortExprs(
        vector<AttributeExpression*> &fakeExprs,
        SortExpressionVector &sortExprs,
        const string &sortFlagStr)
{
    vector<bool> sortFlags;
    StringTokenizer st(sortFlagStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        if (st[i] == "false" || st[i] == "f") {
            sortFlags.push_back(false);
        } else {
            sortFlags.push_back(true);
        }
    }

    for (size_t i = 0; i < sortFlags.size(); ++i) {
        AttributeExpression *fakeAttrExpr = POOL_NEW_CLASS(_pool, FakeRankExpression, "ref_" + StringUtil::toString(i));
        fakeExprs.push_back(fakeAttrExpr);
        SortExpression *sortExpr = 
            _sortExprCreator->createSortExpression(fakeAttrExpr, sortFlags[i]);
        sortExprs.push_back(sortExpr);
        fakeAttrExpr->allocate(_allocatorPtr.get());
    }
}

END_HA3_NAMESPACE(search);


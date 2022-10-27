#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <ha3/rank/DistinctHitCollector.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/rank/test/FakeExpression.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <matchdoc/Reference.h>
#include <ha3/common/MultiErrorResult.h>
#include <ha3/rank/DistinctInfo.h>
#include <ha3/search/Filter.h>
#include <suez/turing/expression/framework/AttributeExpressionPool.h>
#include <ha3/search/SortExpressionCreator.h>
#include <ha3/rank/test/DistinctMapTest.h>
#include <ha3/rank/DistinctHitCollector.h>
#include <ha3/rank/test/FakeExpression.h>
#include <memory>
#include <iostream>
#include <fstream>
#include <ha3/test/test.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/test/FakeRankExpression.h>
#include <ha3/search/test/FakeAttributeExpression.h>
#include <suez/turing/expression/framework/SimpleAttributeExpressionCreator.h>
#include <suez/turing/expression/framework/AtomicAttributeExpression.h>
#include <suez/turing/expression/framework/ComboAttributeExpression.h>
#include <autil/StringUtil.h>
#include <ha3/rank/ComparatorCreator.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(rank);

class DistinctHitCollectorTest : public TESTBASE  {
public:
    DistinctHitCollectorTest();
    ~DistinctHitCollectorTest();
public:
    void setUp();
    void tearDown();
protected:
    void collectMatchDoc(docid_t docid, uint32_t key, float score,
                         DistinctHitCollector &collector, bool isFiltered = false);
    void pushOneItem(DistinctHitCollector &collector, docid_t docid,
                     float score);
    matchdoc::MatchDoc createMatchDoc(docid_t docid, float score);
    void testCollectorDocIdResult(DistinctHitCollector &collector,
                                  const std::string &docIdStr);
    void createFilter();
    void checkDistInfoList(TreeNode *tn, const std::string &docIdStr);
    void addDistinctInfoToTreeNode(TreeNode* treeNode, docid_t docid,
                                   float score, uint32_t gradeBoost);
    DistinctInfo* createDistinctInfo(docid_t docid, float score, uint32_t gradeBoost);
    void checkRemovedDistInfo(DistinctInfo *distInfo, docid_t docid, uint32_t gradeBoost = 0);
protected:
    common::Ha3MatchDocAllocatorPtr _allocatorPtr;
    common::MultiErrorResultPtr _errorResult;
    FakeRankExpression *_expr;
    FakeExpression *_fakeKeyExpr;
    ComboComparator *_cmp;
    AttributeExpressionTyped<bool> *_filterExpr;
    const matchdoc::Reference<score_t> *_scoreRef;
    const matchdoc::Reference<int32_t> *_keyRef;
    std::vector<matchdoc::MatchDoc> _mdVector;
    matchdoc::Reference<DistinctInfo> *_distinctInfoRef;
    search::Filter *_filter;
    std::vector<bool> _filterValues;
    DistinctParameter *_dp;
    autil::mem_pool::Pool *_pool;
    search::SortExpressionCreator *_sortExpressionCreator;
    search::SortExpression *_keySortExpr;
    search::SortExpression *_sortExpr;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, DistinctHitCollectorTest);


DistinctHitCollectorTest::DistinctHitCollectorTest() {
    _expr = NULL;
    _fakeKeyExpr = NULL;
    _keyRef = NULL;
    _scoreRef = NULL;
    _distinctInfoRef = NULL;
    _filter = NULL;
    _filterExpr = NULL;
    _dp = NULL;
    _pool = NULL;
    _sortExpressionCreator = NULL;
    _keySortExpr = NULL;
    _sortExpr = NULL;
}

DistinctHitCollectorTest::~DistinctHitCollectorTest() {
}

void DistinctHitCollectorTest::setUp() {
    _pool = new mem_pool::Pool(1024);
    _sortExpressionCreator = new SortExpressionCreator(NULL, NULL,
            NULL, _errorResult, _pool);
    common::Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(_pool));
    _allocatorPtr = allocatorPtr;
    _expr = new FakeRankExpression("fake_ref");
    // sort pattern: sort=-fake_ref;
    _sortExpr = _sortExpressionCreator->createSortExpression(_expr, false);
    bool ret = _expr->allocate(_allocatorPtr.get());
    ASSERT_TRUE(ret);
    _fakeKeyExpr = new FakeExpression();
    ret = _fakeKeyExpr->allocate(_allocatorPtr.get());
    ASSERT_TRUE(ret);
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVec;
    sortExprVec.push_back(_sortExpr);
    _cmp = creator.createSortComparator(sortExprVec);

    _keyRef = _fakeKeyExpr->getReference();
    _scoreRef = _expr->getReference();
    _distinctInfoRef = allocatorPtr->declare<DistinctInfo>("distinct_info");
    _keySortExpr =
        _sortExpressionCreator->createSortExpression(_fakeKeyExpr, false);
    _dp = new DistinctParameter(1, 1, 0, true, false,
                                _keySortExpr, _distinctInfoRef, NULL);
}

void DistinctHitCollectorTest::tearDown() {
    DELETE_AND_SET_NULL(_expr);
    DELETE_AND_SET_NULL(_fakeKeyExpr);
    DELETE_AND_SET_NULL(_filterExpr);
    DELETE_AND_SET_NULL(_dp);

    for (vector<matchdoc::MatchDoc>::iterator it = _mdVector.begin();
         it != _mdVector.end(); it++)
    {
        if (matchdoc::INVALID_MATCHDOC != *it) {
            _allocatorPtr->deallocate(*it);
        }
    }
    _mdVector.clear();

    _allocatorPtr.reset();
    _filter = NULL;
    _filterValues.clear();
    DELETE_AND_SET_NULL(_sortExpressionCreator);
    DELETE_AND_SET_NULL(_pool);
}

TEST_F(DistinctHitCollectorTest, testDistinctHitCollector) {
    HA3_LOG(DEBUG, "Begin Test!");
    //10 hit, rerank top 100, distinctTimes is 1,  distinctCount is 1
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.0f, collector);
    collectMatchDoc(2, 111, 4.0f, collector);
    collectMatchDoc(3, 333, 2.0f, collector);
    collectMatchDoc(4, 333, 3.0f, collector);
    collectMatchDoc(5, 111, 3.5f, collector);

    ASSERT_EQ((uint32_t)5, collector.getItemCount());

    HA3_LOG(DEBUG, "Start poping ...");
    testCollectorDocIdResult(collector, "3,5,2,4,1");
}

TEST_F(DistinctHitCollectorTest, testMatchDocOverflow) {
    HA3_LOG(DEBUG, "Begin Test!");
    DistinctHitCollector collector(2, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.0f, collector);
    collectMatchDoc(2, 111, 4.0f, collector);
    collectMatchDoc(3, 333, 2.0f, collector);
    collectMatchDoc(4, 333, 3.0f, collector);
    ASSERT_EQ((uint32_t)2, collector.getItemCount());
    testCollectorDocIdResult(collector, "4,1");
}

TEST_F(DistinctHitCollectorTest, testMatchDocOverflow2) {
    HA3_LOG(DEBUG, "Begin Test!");
    DistinctHitCollector collector(2, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 1.0f, collector);
    collectMatchDoc(2, 222, 2.0f, collector);
    collectMatchDoc(3, 333, 3.0f, collector);
    collectMatchDoc(0, 000, 4.0f, collector);
    ASSERT_EQ((uint32_t)2, collector.getItemCount());
    testCollectorDocIdResult(collector, "3,0");
}

TEST_F(DistinctHitCollectorTest, testMatchDocOverflow3) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 2;
    _dp->distinctTimes = 2;
    DistinctHitCollector collector(4, _pool, _allocatorPtr,
                                   _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 1.0f, collector);
    collectMatchDoc(2, 222, 2.0f, collector);
    collectMatchDoc(3, 333, 3.0f, collector);
    collectMatchDoc(4, 444, 4.0f, collector);

    DistinctMap* distMap = collector.getDistinctMap();
    string treeStr1 = DistinctMapTest::treeNodeToString(distMap->getRoot());

    collectMatchDoc(5, 222, 0.5f, collector);

    string treeStr2 = DistinctMapTest::treeNodeToString(distMap->getRoot());
    ASSERT_EQ(treeStr1, treeStr2);

    ASSERT_EQ((uint32_t)4, collector.getItemCount());
    testCollectorDocIdResult(collector, "1,2,3,4");
}

TEST_F(DistinctHitCollectorTest, testMatchDocOverflow4) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 2;
    _dp->distinctTimes = 2;
    DistinctHitCollector collector(3, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 1.0f, collector);
    collectMatchDoc(2, 222, 2.0f, collector);
    collectMatchDoc(3, 111, 1.0f, collector);
    collectMatchDoc(4, 444, 4.0f, collector);

    ASSERT_EQ((uint32_t)3, collector.getItemCount());
    testCollectorDocIdResult(collector, "1,2,4");
}

TEST_F(DistinctHitCollectorTest, testMatchDocOverflow5) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 2;
    _dp->distinctTimes = 2;
    DistinctHitCollector collector(3, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 1.0f, collector);
    collectMatchDoc(2, 222, 2.0f, collector);
    collectMatchDoc(3, 222, 2.0f, collector);
    collectMatchDoc(4, 444, 4.0f, collector);
    collectMatchDoc(5, 444, 3.0f, collector);

    ASSERT_EQ((uint32_t)3, collector.getItemCount());
    testCollectorDocIdResult(collector, "2,5,4");
}

TEST_F(DistinctHitCollectorTest, testMapNodeOverflow) {
    HA3_LOG(DEBUG, "Begin Test!");
    DistinctHitCollector collector(2, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 333, 5.0f, collector);
    collectMatchDoc(2, 333, 4.0f, collector);
    collectMatchDoc(3, 111, 2.0f, collector);
    collectMatchDoc(4, 111, 3.0f, collector);
    collectMatchDoc(5, 444, 6.0f, collector);
    collectMatchDoc(6, 555, 7.0f, collector);
    ASSERT_EQ((uint32_t)2, collector.getItemCount());
    testCollectorDocIdResult(collector, "5,6");
}

TEST_F(DistinctHitCollectorTest, testExtractManyTimes) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 2;
    _dp->distinctTimes = 2;
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.1f, collector);
    collectMatchDoc(2, 111, 5.2f, collector);
    collectMatchDoc(3, 111, 5.3f, collector);
    collectMatchDoc(4, 111, 5.4f, collector);
    collectMatchDoc(5, 111, 5.5f, collector);
    collectMatchDoc(6, 111, 5.6f, collector);
    collectMatchDoc(7, 111, 15.7f, collector);

    collectMatchDoc(11, 222, 10.1f, collector);
    collectMatchDoc(12, 222, 10.2f, collector);
    collectMatchDoc(13, 222, 10.3f, collector);
    collectMatchDoc(14, 222, 10.4f, collector);
    collectMatchDoc(15, 222, 10.5f, collector);
    collectMatchDoc(16, 222, 10.6f, collector);
    collectMatchDoc(17, 222, 10.7f, collector);

    ASSERT_EQ((uint32_t)14, collector.getItemCount());
    testCollectorDocIdResult(collector, "1,2,3,11,12,13,4,5,14,15,6,16,17,7");
}

TEST_F(DistinctHitCollectorTest, testPushMatchDocWithQueuePosition) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 0;
    _dp->distinctTimes = 0;
    DistinctHitCollector queue(10, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);

    matchdoc::MatchDoc item1 = createMatchDoc(1, 0.5f);
    matchdoc::MatchDoc retItem = matchdoc::INVALID_MATCHDOC;

    MatchDocPriorityQueue::PUSH_RETURN_CODE ret
        = queue._queue->push(item1, &retItem);
    ASSERT_EQ(MatchDocPriorityQueue::ITEM_ACCEPTED, ret);
    ASSERT_EQ((uint32_t)1, queue.getQueuePosition(item1));

    matchdoc::MatchDoc item2 = createMatchDoc(10, 7.5f);
    ret = queue._queue->push(item2, &retItem);
    ASSERT_EQ(MatchDocPriorityQueue::ITEM_ACCEPTED, ret);
    ASSERT_EQ((uint32_t)2, queue.getQueuePosition(item2));

    matchdoc::MatchDoc item3 = createMatchDoc(123, 5.6f);
    ret = queue._queue->push(item3, &retItem);
    ASSERT_EQ(MatchDocPriorityQueue::ITEM_ACCEPTED, ret);
    ASSERT_EQ((uint32_t)3, queue.getQueuePosition(item3));

    matchdoc::MatchDoc popItem = queue.popItem();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != popItem);
    _mdVector.push_back(popItem);
    ASSERT_EQ((docid_t)1, popItem.getDocId());

    popItem = queue.popItem();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != popItem);
    _mdVector.push_back(popItem);
    ASSERT_EQ((docid_t)123, popItem.getDocId());

    popItem = queue.popItem();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != popItem);
    _mdVector.push_back(popItem);
    ASSERT_EQ((docid_t)10, popItem.getDocId());
}

TEST_F(DistinctHitCollectorTest, testNotReserveResult) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->reservedFlag = false;
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.1f, collector);
    collectMatchDoc(2, 111, 5.2f, collector);
    collectMatchDoc(3, 111, 5.3f, collector);
    collectMatchDoc(4, 111, 5.4f, collector);
    collectMatchDoc(5, 111, 5.5f, collector);
    collectMatchDoc(6, 111, 5.6f, collector);
    collectMatchDoc(7, 111, 15.7f, collector);

    collectMatchDoc(11, 222, 10.1f, collector);
    collectMatchDoc(12, 222, 10.2f, collector);
    collectMatchDoc(13, 222, 10.3f, collector);
    collectMatchDoc(14, 222, 10.4f, collector);
    collectMatchDoc(15, 222, 10.5f, collector);
    collectMatchDoc(16, 222, 10.6f, collector);
    collectMatchDoc(17, 222, 10.7f, collector);

    ASSERT_EQ((uint32_t)2, collector.getItemCount());
    testCollectorDocIdResult(collector, "17,7");
}

TEST_F(DistinctHitCollectorTest, testNotReserveResult_2_2) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 2;
    _dp->distinctTimes = 2;
    _dp->reservedFlag = false;
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.1f, collector);
    collectMatchDoc(2, 111, 5.2f, collector);
    collectMatchDoc(3, 111, 5.3f, collector);
    collectMatchDoc(4, 111, 5.4f, collector);
    collectMatchDoc(5, 111, 5.5f, collector);
    collectMatchDoc(6, 111, 5.6f, collector);
    collectMatchDoc(7, 111, 15.7f, collector);

    collectMatchDoc(11, 222, 10.1f, collector);
    collectMatchDoc(12, 222, 10.2f, collector);
    collectMatchDoc(13, 222, 10.3f, collector);
    collectMatchDoc(14, 222, 10.4f, collector);
    collectMatchDoc(15, 222, 10.5f, collector);
    collectMatchDoc(16, 222, 10.6f, collector);
    collectMatchDoc(17, 222, 10.7f, collector);

    ASSERT_EQ((uint32_t)8, collector.getItemCount());
    ASSERT_EQ((uint32_t)0, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "4,5,14,15,6,16,17,7");
}

TEST_F(DistinctHitCollectorTest, testNotReserveResultUpdateTotalHit_2_2) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 2;
    _dp->distinctTimes = 2;
    _dp->maxItemCount = 100;
    _dp->reservedFlag = false;
    _dp->updateTotalHitFlag = true;
    DistinctHitCollector collector(5, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.1f, collector);
    collectMatchDoc(2, 111, 5.2f, collector);
    collectMatchDoc(3, 111, 5.3f, collector);
    collectMatchDoc(4, 111, 5.4f, collector);
    collectMatchDoc(5, 111, 5.5f, collector);
    collectMatchDoc(6, 111, 5.6f, collector);
    collectMatchDoc(7, 111, 15.7f, collector);

    collectMatchDoc(11, 222, 10.1f, collector);
    collectMatchDoc(12, 222, 10.2f, collector);
    collectMatchDoc(13, 222, 10.3f, collector);
    collectMatchDoc(14, 222, 10.4f, collector);
    collectMatchDoc(15, 222, 10.5f, collector);
    collectMatchDoc(16, 222, 10.6f, collector);
    collectMatchDoc(17, 222, 10.7f, collector);

    ASSERT_EQ((uint32_t)8, collector.getItemCount());
    ASSERT_EQ((uint32_t)6, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "4,5,14,15,6,16,17,7");
}

TEST_F(DistinctHitCollectorTest, testReplaceElement) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->reservedFlag = false;
    DistinctHitCollector collector(7, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    int itemCount = 0;
    for (int i = 0; i < 7; i++) {
        pushOneItem(collector, i, 100.f - itemCount);
        itemCount++;
    }
    DistinctInfo *info = createDistinctInfo(0, 0, 0);
    info->setQueuePosition(2);
    matchdoc::MatchDoc matchDoc = collector.replaceElement(info, createMatchDoc(9, 101.1f));
    ASSERT_EQ((uint32_t)7, collector.getItemCount());

    testCollectorDocIdResult(collector, "6,5,4,2,1,0,9");
    _allocatorPtr->deallocate(matchDoc);
}

TEST_F(DistinctHitCollectorTest, testCollectLargeNumberDocs) {
    HA3_LOG(DEBUG, "Begin Test!");
    string path = TEST_DATA_PATH;
    path += "/dist_test_data.txt";

    ifstream fin(path.c_str());
    if (!fin.is_open()) {
        HA3_LOG(ERROR, "open test data file failed![%s]", path.c_str());
        ASSERT_TRUE(false);
        return;
    }

    int docCount = 0;
    docid_t docId;
    int32_t distKey;
    float score;
    _dp->distinctCount = 2;
    _dp->distinctTimes = 2;

    DistinctHitCollector collector(10, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    while (fin >> docId >> distKey >> score) {
        docCount ++;
        collectMatchDoc(docId, distKey, score, collector);
    }

    ASSERT_EQ((uint32_t)10, collector.getItemCount());
}

TEST_F(DistinctHitCollectorTest, testSimpleDistinctFilter) {
    HA3_LOG(DEBUG, "Begin Test!");
    createFilter();

    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, _filter);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 111, 5.1f, collector, true);
    collectMatchDoc(2, 111, 5.2f, collector, true);
    collectMatchDoc(3, 222, 5.1f, collector);
    collectMatchDoc(4, 222, 5.4f, collector);
    collectMatchDoc(5, 333, 5.0f, collector);
    collectMatchDoc(6, 333, 5.6f, collector);

    ASSERT_EQ((uint32_t)6, collector.getItemCount());
    ASSERT_EQ((uint32_t)0, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "5,3,1,2,4,6");
}

TEST_F(DistinctHitCollectorTest, testDistinctFilterWithLessTopK) {
    HA3_LOG(DEBUG, "Begin Test!");
    createFilter();
    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, _filter);
    DistinctHitCollector collector(3, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 111, 5.1f, collector, true);
    collectMatchDoc(2, 111, 5.2f, collector, true);
    collectMatchDoc(3, 222, 5.1f, collector);
    collectMatchDoc(4, 222, 5.4f, collector);
    collectMatchDoc(5, 333, 5.0f, collector);
    collectMatchDoc(6, 333, 5.6f, collector);

    ASSERT_EQ((uint32_t)3, collector.getItemCount());
    ASSERT_EQ((uint32_t)0, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "2,4,6");
}

TEST_F(DistinctHitCollectorTest, testDistinctFilterNotReserved) {
    HA3_LOG(DEBUG, "Begin Test!");
    createFilter();
    DistinctParameter dp(1, 1, 0, false, true, _keySortExpr, _distinctInfoRef, _filter);
    DistinctHitCollector collector(6, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 111, 5.1f, collector, true);
    collectMatchDoc(2, 111, 5.2f, collector, true);
    collectMatchDoc(3, 222, 5.1f, collector);
    collectMatchDoc(4, 222, 5.4f, collector);
    collectMatchDoc(5, 333, 5.0f, collector);
    collectMatchDoc(6, 333, 5.6f, collector);

    ASSERT_EQ((uint32_t)4, collector.getItemCount());
    ASSERT_EQ((uint32_t)2, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "1,2,4,6");
}


TEST_F(DistinctHitCollectorTest, testDistinctFilterAllFiltered) {
    HA3_LOG(DEBUG, "Begin Test!");
    createFilter();
    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, _filter);
    DistinctHitCollector collector(6, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 111, 5.15f, collector, true);
    collectMatchDoc(2, 111, 5.2f, collector, true);
    collectMatchDoc(3, 222, 5.1f, collector, true);
    collectMatchDoc(4, 222, 5.4f, collector, true);
    collectMatchDoc(5, 333, 5.0f, collector, true);
    collectMatchDoc(6, 333, 5.6f, collector, true);

    ASSERT_EQ((uint32_t)6, collector.getItemCount());
    testCollectorDocIdResult(collector, "5,3,1,2,4,6");
}

TEST_F(DistinctHitCollectorTest, testMultiDistinctWithFilter_Core) {
    HA3_LOG(DEBUG, "Begin Test!");
    createFilter();
    DistinctParameter dp(1, 1000, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    DistinctHitCollector collector(50000, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    for (size_t i = 0; i < 1; i++) {
        for (size_t j = 0; j < 500; j++) {
            collectMatchDoc(i * j, j, i * j, collector, (i * j) % 5 == 0);
        }
    }
    matchdoc::MatchDoc s = matchdoc::INVALID_MATCHDOC;
    do {
        s = collector.popItem();
        _mdVector.push_back(s);
    } while(matchdoc::INVALID_MATCHDOC != s);

    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVec;
    sortExprVec.push_back(_sortExpr);
    auto cmp = creator.createSortComparator(sortExprVec);

    DistinctParameter dp2(1, 100, 0, true, true, _keySortExpr, _distinctInfoRef, _filter);
    DistinctHitCollector collector2(200, _pool, _allocatorPtr, _expr, cmp, dp2, false);
    for (const auto matchDoc : _mdVector) {
        if (matchdoc::INVALID_MATCHDOC != matchDoc) {
            collector2.collect(matchDoc);
        }
    }
    collector2.flush();
}

TEST_F(DistinctHitCollectorTest, testDistinctFilterWithNoFilteredKey) {
    HA3_LOG(DEBUG, "Begin Test!");
    createFilter();
    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, _filter);
    DistinctHitCollector collector(3, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 5.1f, collector, true);
    collectMatchDoc(2, 22, 5.2f, collector, true);
    collectMatchDoc(3, 33, 5.1f, collector, true);
    collectMatchDoc(4, 44, 5.4f, collector);
    collectMatchDoc(5, 55, 6.0f, collector);
    collectMatchDoc(6, 66, 5.6f, collector);
    collectMatchDoc(7, 66, 5.7f, collector);

    ASSERT_EQ((uint32_t)3, collector.getItemCount());
    testCollectorDocIdResult(collector, "4,7,5");
}

TEST_F(DistinctHitCollectorTest, testDistinctFilterWithMultiTimes) {
    HA3_LOG(DEBUG, "Begin Test!");
    createFilter();
    DistinctParameter dp(2, 3, 0, true, true, _keySortExpr, _distinctInfoRef, _filter);
    DistinctHitCollector collector(8, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 5.1f, collector, true);
    collectMatchDoc(2, 22, 5.2f, collector);
    collectMatchDoc(3, 33, 5.1f, collector, true);
    collectMatchDoc(4, 55, 5.4f, collector);
    collectMatchDoc(5, 55, 6.0f, collector);
    collectMatchDoc(6, 66, 4.0f, collector);
    collectMatchDoc(7, 66, 5.5f, collector);
    collectMatchDoc(8, 66, 5.6f, collector);
    collectMatchDoc(9, 66, 5.7f, collector);
    collectMatchDoc(10, 77, 10.0f, collector, true);
    collectMatchDoc(11, 77, 5.3f, collector, true);
    collectMatchDoc(12, 66, 8.0f, collector);
    collectMatchDoc(13, 66, 6.0f, collector);
    collectMatchDoc(14, 66, 3.0f, collector);

    ASSERT_EQ((uint32_t)8, collector.getItemCount());
    testCollectorDocIdResult(collector, "2,11,4,9,13,5,12,10");
}

TEST_F(DistinctHitCollectorTest, testDistinctCollectorWithGrade) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    dp.gradeThresholds.push_back(1.0f);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 0.2f, collector);
    collectMatchDoc(2, 22, 0.3f, collector);

    collectMatchDoc(3, 22, 1.0f, collector);
    collectMatchDoc(4, 22, 1.1f, collector);

    collectMatchDoc(5, 33, 2.0f, collector);
    collectMatchDoc(6, 33, 1.2f, collector);

    collectMatchDoc(7, 11, 0.1f, collector);
    collectMatchDoc(8, 22, 0.4f, collector);

    ASSERT_EQ((uint32_t)8, collector.getItemCount());
    testCollectorDocIdResult(collector, "7,2,1,8,3,6,4,5");
}

TEST_F(DistinctHitCollectorTest, testDistinctCollectorWithGradeAndMultiDistinctTimes) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(2, 2, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    dp.gradeThresholds.push_back(1.0f);
    dp.gradeThresholds.push_back(2.0f);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 0.2f, collector);
    collectMatchDoc(2, 22, 0.5f, collector);
    collectMatchDoc(3, 22, 0.3f, collector);
    collectMatchDoc(4, 22, 0.7f, collector);
    collectMatchDoc(5, 22, 0.9f, collector);

    collectMatchDoc(6, 22, 1.0f, collector);
    collectMatchDoc(7, 22, 1.1f, collector);
    collectMatchDoc(8, 33, 1.3f, collector);
    collectMatchDoc(9, 33, 1.7f, collector);
    collectMatchDoc(10, 33, 1.9f, collector);

    collectMatchDoc(11, 33, 2.0f, collector);
    collectMatchDoc(12, 33, 1.2f, collector);

    collectMatchDoc(13, 11, 0.1f, collector);
    collectMatchDoc(14, 22, 0.4f, collector);
    collectMatchDoc(15, 22, 0.6f, collector);
    collectMatchDoc(16, 22, 0.8f, collector);

    collectMatchDoc(17, 22, 2.0f, collector);
    collectMatchDoc(18, 22, 2.1f, collector);
    collectMatchDoc(19, 33, 2.3f, collector);
    collectMatchDoc(20, 33, 2.7f, collector);
    collectMatchDoc(21, 33, 2.9f, collector);

    collectMatchDoc(22, 33, 2.0f, collector);
    collectMatchDoc(23, 33, 2.2f, collector);

    ASSERT_EQ((uint32_t)23, collector.getItemCount());
    ASSERT_EQ((uint32_t)0, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "3,14,2,15,4,13,1,16,5,12,8,6,7,9,10,22,11,23,19,17,18,20,21");
}

TEST_F(DistinctHitCollectorTest, testDistinctCollectorWithGradeAndNumOutCollectorLimit) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    dp.gradeThresholds.push_back(1.0f);
    DistinctHitCollector collector(6, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 0.2f, collector);
    collectMatchDoc(2, 22, 0.3f, collector);

    collectMatchDoc(3, 22, 1.0f, collector);
    collectMatchDoc(4, 22, 1.1f, collector);

    collectMatchDoc(5, 33, 2.0f, collector);
    collectMatchDoc(6, 33, 1.2f, collector);

    collectMatchDoc(7, 11, 0.1f, collector);
    collectMatchDoc(8, 22, 0.4f, collector);


    ASSERT_EQ((uint32_t)6, collector.getItemCount());
    ASSERT_EQ((uint32_t)0, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "1,8,3,6,4,5");
}

TEST_F(DistinctHitCollectorTest, testDistinctCollectorWithGradeAndReservedFalse) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(1, 1, 0, false, true, _keySortExpr, _distinctInfoRef, NULL);
    dp.gradeThresholds.push_back(1.0f);
    DistinctHitCollector collector(8, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 0.2f, collector);
    collectMatchDoc(2, 22, 0.3f, collector);

    collectMatchDoc(3, 22, 1.0f, collector);
    collectMatchDoc(4, 22, 1.1f, collector);

    collectMatchDoc(5, 33, 2.0f, collector);
    collectMatchDoc(6, 33, 1.2f, collector);

    collectMatchDoc(7, 11, 0.1f, collector);
    collectMatchDoc(8, 22, 0.4f, collector);


    ASSERT_EQ((uint32_t)4, collector.getItemCount());
    ASSERT_EQ((uint32_t)4, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "1,8,4,5");
}

TEST_F(DistinctHitCollectorTest, testDistinctCollectorWithGradeAndFilter) {
    HA3_LOG(DEBUG, "Begin Test!");

    createFilter();
    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, _filter);
    dp.gradeThresholds.push_back(1.0f);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 0.2f, collector);
    collectMatchDoc(2, 22, 0.3f, collector);

    collectMatchDoc(3, 22, 1.0f, collector);
    collectMatchDoc(4, 22, 1.1f, collector);

    collectMatchDoc(5, 33, 2.0f, collector, true);
    collectMatchDoc(6, 33, 1.2f, collector);

    collectMatchDoc(7, 11, 0.1f, collector);
    collectMatchDoc(8, 22, 0.4f, collector);


    ASSERT_EQ((uint32_t)8, collector.getItemCount());
    testCollectorDocIdResult(collector, "7,2,1,8,3,4,6,5");
}

TEST_F(DistinctHitCollectorTest, testDistinctCollectorWithGradeAndReverseFlag) {
    HA3_LOG(DEBUG, "Begin Test!");

    bool flag = _sortExpr->getSortFlag();
    _sortExpr->setSortFlag(!flag);

    POOL_DELETE_CLASS(_cmp);
    ComparatorCreator creator(_pool, false);
    SortExpressionVector sortExprVec;
    sortExprVec.push_back(_sortExpr);
    _cmp = creator.createSortComparator(sortExprVec);
    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    dp.gradeThresholds.push_back(1.0f);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, !flag);

    collectMatchDoc(1, 11, 0.2f, collector);
    collectMatchDoc(2, 22, 0.3f, collector);

    collectMatchDoc(3, 22, 1.0f, collector);
    collectMatchDoc(4, 22, 1.1f, collector);

    collectMatchDoc(5, 33, 2.0f, collector);
    collectMatchDoc(6, 33, 1.2f, collector);

    collectMatchDoc(7, 11, 0.1f, collector);
    collectMatchDoc(8, 22, 0.4f, collector);

    ASSERT_EQ((uint32_t)8, collector.getItemCount());
    ASSERT_EQ((uint32_t)0, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "5,4,6,3,8,1,2,7");
}

TEST_F(DistinctHitCollectorTest, testAdjustDistinctInfosInList_1_1) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    TreeNode tn;
    DistinctInfo *distInfo = NULL;
    addDistinctInfoToTreeNode(&tn, 1, 1.0, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "1");

    addDistinctInfoToTreeNode(&tn, 2, 0.9, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 2);
    checkDistInfoList(&tn, "1");

    addDistinctInfoToTreeNode(&tn, 3, 1.0, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 3);
    checkDistInfoList(&tn, "1");

    addDistinctInfoToTreeNode(&tn, 4, 1.1, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 1);
    checkDistInfoList(&tn, "4");
}

TEST_F(DistinctHitCollectorTest, testAdjustDistinctInfosInList_2_1) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(2, 1, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    TreeNode tn;
    DistinctInfo *distInfo;
    addDistinctInfoToTreeNode(&tn, 1, 1.0, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "1");

    addDistinctInfoToTreeNode(&tn, 2, 0.9, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "2,1");

    addDistinctInfoToTreeNode(&tn, 3, 0.8, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 3);
    checkDistInfoList(&tn, "2,1");

    addDistinctInfoToTreeNode(&tn, 5, 0.9, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 5);
    checkDistInfoList(&tn, "2,1");

    addDistinctInfoToTreeNode(&tn, 4, 0.9, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 4);
    checkDistInfoList(&tn, "2,1");

    addDistinctInfoToTreeNode(&tn, 6, 0.95, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 2);
    checkDistInfoList(&tn, "6,1");

    addDistinctInfoToTreeNode(&tn, 7, 1.1, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 6);
    checkDistInfoList(&tn, "1,7");
}

TEST_F(DistinctHitCollectorTest, testAdjustDistinctInfosInListWithGrade_2_1) {
    HA3_LOG(DEBUG, "Begin Test!");

    vector<double> grades;
    grades.push_back(1.0);
    DistinctParameter dp(2, 2, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    dp.gradeThresholds = grades;
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    TreeNode tn;
    DistinctInfo *distInfo;
    addDistinctInfoToTreeNode(&tn, 1, 0.5, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "1");

    addDistinctInfoToTreeNode(&tn, 2, 1.5, 1);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "1;2");

    addDistinctInfoToTreeNode(&tn, 3, 0.4, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "3,1;2");

    addDistinctInfoToTreeNode(&tn, 4, 1.3, 1);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "3,1;4,2");

    addDistinctInfoToTreeNode(&tn, 5, 0.7, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "3,1,5;4,2");

    addDistinctInfoToTreeNode(&tn, 6, 0.6, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "3,1,6,5;4,2");

    addDistinctInfoToTreeNode(&tn, 7, 0.0, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 7);
    checkDistInfoList(&tn, "3,1,6,5;4,2");

    addDistinctInfoToTreeNode(&tn, 8, 0.9, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 3);
    checkDistInfoList(&tn, "1,6,5,8;4,2");

    addDistinctInfoToTreeNode(&tn, 9, 0.65, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 1);
    checkDistInfoList(&tn, "6,9,5,8;4,2");

    addDistinctInfoToTreeNode(&tn, 10, 1.4, 1);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "6,9,5,8;4,10,2");

    addDistinctInfoToTreeNode(&tn, 11, 1.6, 1);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "6,9,5,8;4,10,2,11");

    addDistinctInfoToTreeNode(&tn, 12, 1.0, 1);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 12, 1);
    checkDistInfoList(&tn, "6,9,5,8;4,10,2,11");

    addDistinctInfoToTreeNode(&tn, 13, 2.0, 1);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 4, 1);
    checkDistInfoList(&tn, "6,9,5,8;10,2,11,13");

    addDistinctInfoToTreeNode(&tn, 14, 1.45, 1);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 10, 1);
    checkDistInfoList(&tn, "6,9,5,8;14,2,11,13");
}

TEST_F(DistinctHitCollectorTest, testAdjustDistinctInfosInListAddItemWithPosInfo) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    TreeNode tn;
    DistinctInfo *distInfo = NULL;
    addDistinctInfoToTreeNode(&tn, 1, 1.0, 0);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    ASSERT_TRUE(!distInfo);
    checkDistInfoList(&tn, "1");

    addDistinctInfoToTreeNode(&tn, 2, 0.9, 0);
    DistinctInfo *item = tn._sdi;
    item->setQueuePosition(3);
    distInfo = collector.adjustDistinctInfosInList(&tn);
    checkRemovedDistInfo(distInfo, 2);
    checkDistInfoList(&tn, "1");
}

void DistinctHitCollectorTest::checkRemovedDistInfo(DistinctInfo *distInfo, docid_t docid, uint32_t gradeBoost)
{
    ASSERT_TRUE(distInfo);
    ASSERT_EQ((docid_t)docid, distInfo->getMatchDoc().getDocId());
    ASSERT_TRUE(!distInfo->getTreeNode());
    ASSERT_DOUBLE_EQ((double)0.0, (double)distInfo->getDistinctBoost());
    ASSERT_EQ((uint32_t)gradeBoost, distInfo->getGradeBoost());
}

void DistinctHitCollectorTest::checkDistInfoList(TreeNode *tn, const string &docIdStr) {
    // docIdStr: "docid,docid;docid"
    DistinctInfo *sdi = tn->_sdi;
    uint32_t count = 0;
    const vector<string> &grades = StringUtil::split(docIdStr, ";");
    for (vector<string>::const_iterator gradeIt = grades.begin();
         gradeIt != grades.end(); ++gradeIt)
    {
        const vector<string> &docIds = StringUtil::split(*gradeIt, ",");
        vector<string>::const_iterator it = docIds.begin();
        uint32_t gradePos = (uint32_t)docIds.size();
        count += gradePos;
        for (; it != docIds.end() && sdi != NULL;
             ++it, sdi = sdi->_next)
        {
            ASSERT_TRUE(sdi);
            if (!(StringUtil::fromString<docid_t>(*it) == sdi->getMatchDoc().getDocId())) {
                HA3_LOG(ERROR, "assert failed, error docid");
                assert(false);
            }
            gradePos --;
            stringstream ss;
            ss << "docid:" << sdi->getMatchDoc().getDocId() << ", "
               << "grade:" << sdi->getGradeBoost()
               << endl;
            if (!(gradePos == sdi->getKeyPosition())) {
                HA3_LOG(ERROR, "assert failed, %s", ss.str().c_str());
                assert(false);
            }
        }
    }
    ASSERT_TRUE(!sdi);
    ASSERT_EQ(count, tn->getCount());
}

void DistinctHitCollectorTest::addDistinctInfoToTreeNode(TreeNode* treeNode,
        docid_t docid, float score, uint32_t gradeBoost)
{
    DistinctInfo* distInfo = createDistinctInfo(docid, score, gradeBoost);
    distInfo->_next = treeNode->_sdi;
    treeNode->_sdi = distInfo;
    distInfo->setTreeNode(treeNode);
}

DistinctInfo* DistinctHitCollectorTest::createDistinctInfo(
        docid_t docid, float score, uint32_t gradeBoost)
{
    matchdoc::MatchDoc matchDoc = createMatchDoc(docid, score);
    _mdVector.push_back(matchDoc);
    DistinctInfo* distInfo = _distinctInfoRef->getPointer(matchDoc);
    distInfo->setMatchDoc(matchDoc);
    distInfo->setGradeBoost(gradeBoost);
    return distInfo;
}

void DistinctHitCollectorTest::collectMatchDoc(docid_t docid, uint32_t key,
        float score, DistinctHitCollector &collector, bool isFiltered)
{
    matchdoc::MatchDoc matchDoc = _allocatorPtr->allocate(docid);
    _scoreRef->set(matchDoc, score);
    _keyRef->set(matchDoc, key);
    _filterValues.resize(docid + 1);
    _filterValues[docid] = isFiltered;
    collector.collect(matchDoc);
    collector.flush();
}

void DistinctHitCollectorTest::pushOneItem(DistinctHitCollector &collector,
        docid_t docid, float score)
{
    auto item = createMatchDoc(docid, score);
    auto retItem = matchdoc::INVALID_MATCHDOC;
    MatchDocPriorityQueue::PUSH_RETURN_CODE ret
        = collector._queue->push(item, &retItem);
    ASSERT_EQ(MatchDocPriorityQueue::ITEM_ACCEPTED, ret);
}

matchdoc::MatchDoc DistinctHitCollectorTest::createMatchDoc(docid_t docid, float score)
{
    auto allocator = dynamic_cast<common::Ha3MatchDocAllocator *>(_allocatorPtr.get());
    matchdoc::MatchDoc matchDoc = allocator->allocate(docid);
    _scoreRef->set(matchDoc, score);
    return matchDoc;
}

void DistinctHitCollectorTest::testCollectorDocIdResult(
        DistinctHitCollector &collector, const string &docIdStr)
{
    matchdoc::MatchDoc s = matchdoc::INVALID_MATCHDOC;
    const vector<string> &docIds = StringUtil::split(docIdStr, ",");
    vector<string>::const_iterator it = docIds.begin();
    for (; it != docIds.end(); ++it) {
        s = collector.popItem();
        _mdVector.push_back(s);
        ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != s);
        ASSERT_EQ(StringUtil::fromString<docid_t>(*it),
                             s.getDocId());
    }

    s = collector.popItem();
    _mdVector.push_back(s);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == s);
}

void DistinctHitCollectorTest::createFilter() {
    _filterExpr =
        new FakeAttributeExpression<bool>("filter", &_filterValues);
    _filterExpr->allocate(_allocatorPtr.get());
    _filter = POOL_NEW_CLASS(_pool, Filter, _filterExpr);
}

TEST_F(DistinctHitCollectorTest, testDistinctCollectorWithGradeAndDocNumByondHeapLimit) {
    HA3_LOG(DEBUG, "Begin Test!");

    DistinctParameter dp(1, 1, 0, true, true, _keySortExpr, _distinctInfoRef, NULL);
    dp.gradeThresholds.push_back(0.05f);
    DistinctHitCollector collector(5, _pool, _allocatorPtr, _expr, _cmp, dp, false);

    collectMatchDoc(1, 11, 0.2f, collector);
    collectMatchDoc(2, 22, 0.3f, collector);

    collectMatchDoc(3, 22, 1.0f, collector);
    collectMatchDoc(4, 22, 1.1f, collector);

    collectMatchDoc(7, 11, 0.1f, collector);
    collectMatchDoc(8, 22, 0.4f, collector);

    collectMatchDoc(5, 33, 2.0f, collector);
    collectMatchDoc(6, 33, 1.2f, collector);

    ASSERT_EQ((uint32_t)5, collector.getItemCount());
    testCollectorDocIdResult(collector, "3,6,1,4,5");
}

TEST_F(DistinctHitCollectorTest, testKeyCountGreaterThanSize) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->distinctCount = 1;
    _dp->distinctTimes = 1;
    _dp->maxItemCount = 100;
    _dp->reservedFlag = false;
    _dp->updateTotalHitFlag = true;
    DistinctHitCollector collector(3, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.1f, collector);
    collectMatchDoc(2, 222, 10.1f, collector);
    collectMatchDoc(3, 333, 11.1f, collector);
    collectMatchDoc(4, 444, 12.1f, collector);
    collectMatchDoc(5, 555, 13.1f, collector);
    collectMatchDoc(6, 666, 14.1f, collector);

    ASSERT_EQ((uint32_t)6, collector.getItemCount());
    ASSERT_EQ((uint32_t)0, collector.getDeletedDocCount());
    testCollectorDocIdResult(collector, "1,2,3,4,5,6");
}

TEST_F(DistinctHitCollectorTest, testNotReserveResultOnlyOneKey0) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->reservedFlag = false;
    _dp->distinctCount = 2;
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.0f, collector);
    collectMatchDoc(2, 111, 4.0f, collector);
    collectMatchDoc(3, 111, 6.0f, collector);


    ASSERT_EQ((uint32_t)2, collector.getItemCount());
    testCollectorDocIdResult(collector, "1,3");
}

TEST_F(DistinctHitCollectorTest, testNotReserveResultOnlyOneKey1) {
    HA3_LOG(DEBUG, "Begin Test!");
    _dp->reservedFlag = false;
    _dp->distinctCount = 1;
    DistinctHitCollector collector(100, _pool, _allocatorPtr, _expr, _cmp, *_dp, false);
    collectMatchDoc(1, 111, 5.0f, collector);
    collectMatchDoc(2, 111, 4.0f, collector);
    collectMatchDoc(3, 111, 6.0f, collector);


    ASSERT_EQ((uint32_t)1, collector.getItemCount());
    testCollectorDocIdResult(collector, "3");
}

END_HA3_NAMESPACE(rank);

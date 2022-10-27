#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/Comparator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/MatchDocPriorityQueue.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include <ha3/rank/ReferenceComparator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/MatchDocPriorityQueue.h>
#include <memory>
//#include <ha3/rank/test/MatchDocMaker.h>

using namespace std;
using namespace autil;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);

class MatchDocPriorityQueueTest : public TESTBASE {
public:
    MatchDocPriorityQueueTest();
    ~MatchDocPriorityQueueTest();
public:
    void setUp();
    void tearDown();
protected:
    MatchDocPriorityQueue::PUSH_RETURN_CODE pushOneItem(
            MatchDocPriorityQueue &queue, docid_t docid, float score,
            matchdoc::MatchDoc *retItem);
    matchdoc::MatchDoc createMatchDoc(docid_t docid, float score);

    void internalTestPushAndPop(uint32_t queueSize,
                                const std::string &pushOpteratorStr,
                                const std::string &popResultStr,
                                uint32_t leftCount);
    void checkPush(MatchDocPriorityQueue &queue,
                   const std::string &pushOpteratorStr);
    void checkPop(MatchDocPriorityQueue &queue,
                  const std::string &popOpteratorStr);
    MatchDocPriorityQueue::PUSH_RETURN_CODE pushToQueue(
            MatchDocPriorityQueue &queue, matchdoc::MatchDoc item,
            matchdoc::MatchDoc *retItem);
protected:
    autil::mem_pool::Pool *_pool;
    Comparator *_cmp;
    common::Ha3MatchDocAllocatorPtr _allocatorPtr;
    matchdoc::Reference<float> *_scoreRef;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, MatchDocPriorityQueueTest);


MatchDocPriorityQueueTest::MatchDocPriorityQueueTest() {
    _pool = new autil::mem_pool::Pool(1024);
    common::Ha3MatchDocAllocatorPtr allocatorPtr(new common::Ha3MatchDocAllocator(_pool));
    _allocatorPtr = allocatorPtr;
    _scoreRef = NULL;
    _cmp = NULL;
}

MatchDocPriorityQueueTest::~MatchDocPriorityQueueTest() {
    _allocatorPtr.reset();
    delete _pool;
}

void MatchDocPriorityQueueTest::setUp() {
    _scoreRef = _allocatorPtr->declare<float>("score");
    _cmp = new ReferenceComparator<float>(_scoreRef);
}

void MatchDocPriorityQueueTest::tearDown() {
    if (_cmp) {
        delete _cmp;
    }
}

TEST_F(MatchDocPriorityQueueTest, testNoneItem) {
    HA3_LOG(DEBUG, "Begin Test!");
    internalTestPushAndPop(10, "", "-1", 0);
}

TEST_F(MatchDocPriorityQueueTest, testOneItem) {
    HA3_LOG(DEBUG, "Begin Test!");
    internalTestPushAndPop(2, "345,1.5f,AC", "345,-1", 0);
}

TEST_F(MatchDocPriorityQueueTest, testSixItem) {
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "0,0,AC;"
                             "1,2,AC;"
                             "2,4,AC;"
                             "3,6,AC;"
                             "4,8,AC;"
                             "5,10,AC;"
                             "6,12,AC;";
    string popResultStr = "0,1,2,3,4,5,6,-1";
    internalTestPushAndPop(7, pushOperatorStr, popResultStr, 0);
}

TEST_F(MatchDocPriorityQueueTest, testSevenItem) {
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "0,100,AC;"
                             "1,99,AC;"
                             "2,98,AC;"
                             "3,97,AC;"
                             "4,96,AC;"
                             "5,95,AC;"
                             "6,94,AC;";
    string popResultStr = "6,5,4,3,2,1,0,-1";
    internalTestPushAndPop(123, pushOperatorStr, popResultStr, 0);
}

TEST_F(MatchDocPriorityQueueTest, testEightItem) {
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "0,100,AC;"
                             "1,99,AC;"
                             "2,98,AC;"
                             "3,97,AC;"
                             "4,96,AC;"
                             "5,95,AC;"
                             "6,94,AC;"
                             "7,93,AC;";
    string popResultStr = "7,6,5,4,3,2,1,0,-1";
    internalTestPushAndPop(23, pushOperatorStr, popResultStr, 0);
}

TEST_F(MatchDocPriorityQueueTest, testHaveOneItem) {
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "23,1.5,AC;"
                             "111,0.5,AC;";
    string popResultStr = "111,23,-1";
    internalTestPushAndPop(2, pushOperatorStr, popResultStr, 0);
}

TEST_F(MatchDocPriorityQueueTest, testFullItem) {
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "0,100,AC;"
                             "1,99,AC;"
                             "2,98,AC;"
                             "3,97,AC;"
                             "4,96,AC;"
                             "5,95,AC;"
                             "6,94,AC;"
                             "23,20,DN,23;"
                             "111,1000,RP,6";
    string popResultStr = "";
    internalTestPushAndPop(7, pushOperatorStr, popResultStr, 7);
}

TEST_F(MatchDocPriorityQueueTest, testHeapSortCtl) {
    HA3_LOG(DEBUG, "Begin Test!");
    string pushOperatorStr = "0,100,AC;"
                             "1,99,AC;"
                             "2,98,AC;"
                             "3,97,AC;"
                             "4,96,AC;"
                             "5,95,AC;"
                             "6,94,AC;"
                             "23,20,AC;"
                             "111,1000,AC;";
    string popResultStr = "23,6,5,4,3,2,1,0,111";
    internalTestPushAndPop(10, pushOperatorStr, popResultStr, 0);
}

TEST_F(MatchDocPriorityQueueTest, testTop) {
    HA3_LOG(DEBUG, "Begin test");
    MatchDocPriorityQueue queue(7, _pool, _cmp);
    matchdoc::MatchDoc matchDoc = queue.top();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == matchDoc);

    string pushOperatorStr = "0,100,AC;"
                             "1,99,AC;"
                             "2,98,AC;"
                             "3,97,AC;"
                             "4,96,AC;"
                             "5,95,AC;"
                             "23,20,AC"
                             "6,94,RP,5";
    checkPush(queue, pushOperatorStr);
    matchDoc = queue.top();
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
    ASSERT_EQ((docid_t)23, matchDoc.getDocId());
}

void MatchDocPriorityQueueTest::internalTestPushAndPop(
        uint32_t queueSize, const string &pushOpteratorStr,
        const string &popResultStr, uint32_t leftCount)
{
    MatchDocPriorityQueue queue(queueSize, _pool, _cmp);
    checkPush(queue, pushOpteratorStr);
    checkPop(queue, popResultStr);
    ASSERT_EQ(leftCount, queue.count());
}

void MatchDocPriorityQueueTest::checkPush(MatchDocPriorityQueue &queue,
        const string &pushOpteratorStr)
{
    StringTokenizer st(pushOpteratorStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        StringTokenizer st2(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        assert(st2.getNumTokens() >= 3);
        docid_t docId = StringUtil::fromString<docid_t>(st2[0].c_str());
        float score = StringUtil::fromString<float>(st2[1].c_str());
        MatchDocPriorityQueue::PUSH_RETURN_CODE code = MatchDocPriorityQueue::ITEM_ACCEPTED;
        if (st2[2] == "AC") {
            code = MatchDocPriorityQueue::ITEM_ACCEPTED;
        } else if (st2[2] == "DN") {
            code = MatchDocPriorityQueue::ITEM_DENIED;
        } else if (st2[2] == "RP") {
            code = MatchDocPriorityQueue::ITEM_REPLACED;
        }
        docid_t retDocId = INVALID_DOCID;
        if (st2.getNumTokens() == 4) {
            retDocId = StringUtil::fromString<docid_t>(st2[3].c_str());
        }
        matchdoc::MatchDoc matchDoc = createMatchDoc(docId, score);
        matchdoc::MatchDoc retItem = matchdoc::INVALID_MATCHDOC;
        ASSERT_EQ(code, pushToQueue(queue, matchDoc, &retItem));
        if (retDocId == INVALID_DOCID) {
            ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == retItem);
        } else {
            ASSERT_EQ(retDocId, retItem.getDocId());
            _allocatorPtr->deallocate(retItem);
        }
    }
}

void MatchDocPriorityQueueTest::checkPop(MatchDocPriorityQueue &queue,
        const string &popOpteratorStr)
{
    StringTokenizer st(popOpteratorStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY
                       | StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        docid_t docId = StringUtil::fromString<docid_t>(st[i].c_str());
        matchdoc::MatchDoc item = queue.pop();
        if (docId == INVALID_DOCID) {
            ASSERT_TRUE(matchdoc::INVALID_MATCHDOC == item);
        } else {
            ASSERT_EQ(docId, item.getDocId());
            _allocatorPtr->deallocate(item);
        }
    }
}

class Comp {
public:
    Comp(const Comparator *queueCmp) {
        _queueCmp = queueCmp;
    }
public:
    bool operator()(matchdoc::MatchDoc lft, matchdoc::MatchDoc rht) {
        return _queueCmp->compare(rht, lft);
    }
private:
    const Comparator *_queueCmp;
};

MatchDocPriorityQueue::PUSH_RETURN_CODE
MatchDocPriorityQueueTest::pushToQueue(MatchDocPriorityQueue &queue, matchdoc::MatchDoc item, matchdoc::MatchDoc *retItem)
{
    return queue.push(item, retItem);
}

matchdoc::MatchDoc MatchDocPriorityQueueTest::createMatchDoc(docid_t docid, float score)
{
    matchdoc::MatchDoc matchDoc = _allocatorPtr->allocate(docid);
    _scoreRef->set(matchDoc, score);
    return matchDoc;
}

END_HA3_NAMESPACE(rank);

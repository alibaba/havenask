#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/DistinctInfo.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/DistinctComparator.h>
#include <matchdoc/MatchDoc.h>

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(rank);

class DistinctComparatorTest : public TESTBASE {
public:
    DistinctComparatorTest();
    ~DistinctComparatorTest();

protected:
    matchdoc::MatchDoc createMatchDoc(docid_t docid, float score, float distBoost);
public:
    void setUp();
    void tearDown();
protected:
    matchdoc::MatchDoc _item1;
    matchdoc::MatchDoc _item11;
    matchdoc::MatchDoc _item0;
    matchdoc::MatchDoc _item10;

    matchdoc::Reference<float> *_scoreRef;
    matchdoc::Reference<DistinctInfo> *_distInfoRef;
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_allocator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, DistinctComparatorTest);


DistinctComparatorTest::DistinctComparatorTest() { 
    _item1 = matchdoc::INVALID_MATCHDOC;
    _item0 = matchdoc::INVALID_MATCHDOC;
    _item11 = matchdoc::INVALID_MATCHDOC;
    _item10 = matchdoc::INVALID_MATCHDOC;
    _pool = new autil::mem_pool::Pool(1024);
    _allocator = new common::Ha3MatchDocAllocator(_pool);
    _distInfoRef = _allocator->declare<DistinctInfo>("distinct_info");
    _scoreRef = _allocator->declare<float>("score");
}

DistinctComparatorTest::~DistinctComparatorTest() { 
    delete _allocator;
    delete _pool;
}

void DistinctComparatorTest::setUp() { 
    _item0 = createMatchDoc(102, 0.7f, 0.0);
    _item1 = createMatchDoc(101, 0.5f, 1.0);
    _item10 = createMatchDoc(107, 0.7f, 0.0);
    _item11 = createMatchDoc(107, 0.7f, 1.0);
}

void DistinctComparatorTest::tearDown() { 
    _allocator->deallocate(_item1);
    _allocator->deallocate(_item0);
    _allocator->deallocate(_item11);
    _allocator->deallocate(_item10);
}


TEST_F(DistinctComparatorTest, testDistinctComparator) { 
    HA3_LOG(DEBUG, "Begin Test!");
    DistinctComparator cmp(_distInfoRef);
    ASSERT_TRUE(!cmp.compare(_item1, _item11));
    ASSERT_TRUE(!cmp.compare(_item1, _item1));

    ASSERT_TRUE(!cmp.compare(_item0, _item0));
    ASSERT_TRUE(!cmp.compare(_item10, _item10));

    ASSERT_TRUE(!cmp.compare(_item1, _item0));
    ASSERT_TRUE(cmp.compare(_item0, _item1));
}

matchdoc::MatchDoc DistinctComparatorTest::createMatchDoc(docid_t docid,
        float score, float distBoost)
{
    matchdoc::MatchDoc matchDoc = _allocator->allocate(docid);
    _scoreRef->set(matchDoc, score);
    DistinctInfo &distInfo = _distInfoRef->getReference(matchDoc);
    distInfo.setDistinctBoost(distBoost);
    return matchDoc;
}

END_HA3_NAMESPACE(rank);


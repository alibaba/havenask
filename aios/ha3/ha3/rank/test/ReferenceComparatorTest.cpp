#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/ReferenceComparator.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/rank/Comparator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/rank/ReferenceComparator.h>
#include <string>
#include <matchdoc/MatchDoc.h>
#include <ha3/test/test.h>

using namespace std;
BEGIN_HA3_NAMESPACE(rank);

class ReferenceComparatorTest : public TESTBASE {
public:
    ReferenceComparatorTest();
    ~ReferenceComparatorTest();
public:
    void setUp();
    void tearDown();
protected:
    matchdoc::MatchDoc createMatchDoc(docid_t docid, int32_t score);

protected:
    ReferenceComparator<int32_t> *_referenceCmp;
    matchdoc::MatchDoc _a;
    matchdoc::MatchDoc _b;
    const matchdoc::Reference<int32_t> *_scoreRef;
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_allocator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, ReferenceComparatorTest);


ReferenceComparatorTest::ReferenceComparatorTest() {
    _pool = new autil::mem_pool::Pool(1024);
    _allocator = new common::Ha3MatchDocAllocator(_pool);
    _scoreRef = _allocator->declare<int32_t>("score");

    _a = createMatchDoc(0, 5);
    _b = createMatchDoc(1, 5);
}

ReferenceComparatorTest::~ReferenceComparatorTest() {
    _allocator->deallocate(_a);
    _allocator->deallocate(_b);
    delete _allocator;
    delete _pool;
}

void ReferenceComparatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");

    _referenceCmp = new ReferenceComparator<int32_t>(_scoreRef);
}

void ReferenceComparatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");

    delete _referenceCmp;
}

TEST_F(ReferenceComparatorTest, testBug41525) {
    HA3_LOG(DEBUG, "Begin Test!");

    _referenceCmp->setReverseFlag(true);
    ASSERT_TRUE(!_referenceCmp->compare(_a, _b));
    ASSERT_TRUE(!_referenceCmp->compare(_b, _a));
}

TEST_F(ReferenceComparatorTest, testNormal) {
    HA3_LOG(DEBUG, "Begin Test!");

    matchdoc::MatchDoc c = createMatchDoc(0, 5);
    matchdoc::MatchDoc d = createMatchDoc(1, 4);

    {
        _referenceCmp->setReverseFlag(true);
        ASSERT_TRUE(_referenceCmp->compare(c, d));
    }
    {
        _referenceCmp->setReverseFlag(false);
        ASSERT_TRUE(!_referenceCmp->compare(c, d));
    }
}

matchdoc::MatchDoc ReferenceComparatorTest::createMatchDoc(docid_t docid,
        int32_t score)
{
    matchdoc::MatchDoc matchDoc = _allocator->allocate(docid);
    _scoreRef->set(matchDoc, score);
    return matchDoc;
}

END_HA3_NAMESPACE(rank);

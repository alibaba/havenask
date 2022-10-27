#include<unittest/unittest.h>
#include <ha3/search/test/MatchDocAllocatorTest.h>

BEGIN_HA3_NAMESPACE(search);

class MatchDocAllocatorTest : public TESTBASE {
public:
    MatchDocAllocatorTest();
    ~MatchDocAllocatorTest();
public:
    void setUp();
    void tearDown();
protected:
    MatchDocAllocator _matchDocAllocator;
    rank::MatchDoc *_matchDoc;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, MatchDocAllocatorTest);


MatchDocAllocatorTest::MatchDocAllocatorTest() { 
}

MatchDocAllocatorTest::~MatchDocAllocatorTest() { 
}

void MatchDocAllocatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _matchDoc = NULL;
}

void MatchDocAllocatorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    if (_matchDoc) {
        _matchDocAllocator.deallocateMatchDoc(_matchDoc);
        _matchDoc = NULL;
    }
}

void MatchDocAllocatorTest::testMatchDocAllocator() { 
    HA3_LOG(DEBUG, "Begin Test!");

    _matchDoc = _matchDocAllocator.allocateMatchDoc((docid_t)1);
    ASSERT_TRUE(_matchDoc);
    ASSERT_EQ((void*)_matchDoc, (void*)_matchDoc->getVariableSlab());
}

END_HA3_NAMESPACE(search);


#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <vector>
#include <ha3/search/PhraseInDocPositionState.h>
using namespace std;
IE_NAMESPACE_USE(index);
BEGIN_HA3_NAMESPACE(search);

class PhraseInDocPositionIteratorTest : public TESTBASE {
public:
    PhraseInDocPositionIteratorTest();
    ~PhraseInDocPositionIteratorTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, PhraseInDocPositionIteratorTest);

PhraseInDocPositionIteratorTest::PhraseInDocPositionIteratorTest() { 
}

PhraseInDocPositionIteratorTest::~PhraseInDocPositionIteratorTest() { 
}

void PhraseInDocPositionIteratorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void PhraseInDocPositionIteratorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(PhraseInDocPositionIteratorTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    vector<pos_t> *posVec = new vector<pos_t>;
    posVec->push_back(1);
    PhraseInDocPositionState state(posVec, 1);
    InDocPositionIterator *positionIterator = state.CreateInDocIterator();
    unique_ptr<InDocPositionIterator> ptr(positionIterator);
    ASSERT_EQ((pos_t)1, positionIterator->SeekPosition(INVALID_POSITION)); 
    ASSERT_EQ(INVALID_POSITION, positionIterator->SeekPosition(INVALID_POSITION)); 
    HA3_LOG(DEBUG, "Seek success");

}

END_HA3_NAMESPACE(search);


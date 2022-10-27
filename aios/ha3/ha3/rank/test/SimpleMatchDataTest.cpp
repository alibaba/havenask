#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/rank/SimpleMatchData.h>

BEGIN_HA3_NAMESPACE(rank);

class SimpleMatchDataTest : public TESTBASE {
public:
    SimpleMatchDataTest();
    ~SimpleMatchDataTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(rank, SimpleMatchDataTest);


SimpleMatchDataTest::SimpleMatchDataTest() { 
}

SimpleMatchDataTest::~SimpleMatchDataTest() { 
}

void SimpleMatchDataTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SimpleMatchDataTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SimpleMatchDataTest, testIsMatch) { 
    HA3_LOG(DEBUG, "Begin Test!");

    char *matchDataMem = new char[1024];
    SimpleMatchData *data = new ((void*)matchDataMem)SimpleMatchData;
    memset(matchDataMem, 0xff, 1024);
    for (size_t i = 0; i < 1024 * 8; ++i) {
        ASSERT_TRUE(data->isMatch(i));
    }

    memset(matchDataMem, 0, 1024);
    for (size_t i = 0; i < 1024 * 8; ++i) {
        ASSERT_TRUE(!data->isMatch(i));
    }
    delete []matchDataMem;
}

TEST_F(SimpleMatchDataTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    SimpleMatchData data;
    data.setMatch(0, true);
    data.setMatch(1, false);
    data.setMatch(6, false);
    data.setMatch(13, true);
    data.setMatch(26, true);
    data.setMatch(31, true);

    ASSERT_TRUE(data.isMatch(0));
    ASSERT_TRUE(!data.isMatch(1));
    ASSERT_TRUE(!data.isMatch(6));
    ASSERT_TRUE(data.isMatch(13));
    ASSERT_TRUE(data.isMatch(26));
    ASSERT_TRUE(data.isMatch(31));
}

TEST_F(SimpleMatchDataTest, testBigData) { 
    HA3_LOG(DEBUG, "Begin Test!");
    char *matchDataMem = new char[1024];
    SimpleMatchData *data = new ((void*)matchDataMem)SimpleMatchData;
    // uint32_t maxSize = 1024 * 8;
    data->setMatch(0, true);
    data->setMatch(13, true);
    data->setMatch(4444, true);
    data->setMatch(5555, true);
    data->setMatch(5556, false);

    ASSERT_TRUE(data->isMatch(0));
    ASSERT_TRUE(data->isMatch(13));
    ASSERT_TRUE(data->isMatch(4444));
    ASSERT_TRUE(data->isMatch(5555));
    ASSERT_TRUE(!data->isMatch(5556));
    delete []matchDataMem;
}

TEST_F(SimpleMatchDataTest, testSizeOf) { 
    ASSERT_EQ((uint32_t)0, SimpleMatchData::sizeOf(0));
    for (uint32_t i = 1; i <= 32; ++i) {
        ASSERT_EQ((uint32_t)4, SimpleMatchData::sizeOf(i));
    }
    for (uint32_t i = 33; i <= 64; ++i) {
        ASSERT_EQ((uint32_t)8, SimpleMatchData::sizeOf(i));
    }
}

END_HA3_NAMESPACE(rank);


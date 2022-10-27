#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/FieldBoost.h>
BEGIN_HA3_NAMESPACE(config);

class FieldBoostTest : public TESTBASE {
public:
    FieldBoostTest();
    ~FieldBoostTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, FieldBoostTest);


FieldBoostTest::FieldBoostTest() { 
}

FieldBoostTest::~FieldBoostTest() { 
}

void FieldBoostTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void FieldBoostTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(FieldBoostTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");

    FieldBoost boost;
    ASSERT_EQ((fieldboost_t)0, boost[0]);
    ASSERT_EQ((fieldboost_t)0, boost[1]);
    ASSERT_EQ((fieldboost_t)0, boost[2]);
    ASSERT_EQ((fieldboost_t)0, boost[3]);
    ASSERT_EQ((fieldboost_t)0, boost[4]);
    ASSERT_EQ((fieldboost_t)0, boost[5]);
    ASSERT_EQ((fieldboost_t)0, boost[6]);
    ASSERT_EQ((fieldboost_t)0, boost[7]);
}

TEST_F(FieldBoostTest, testOperatorSetGet) { 
    HA3_LOG(DEBUG, "Begin Test!");
    FieldBoost boost;

    boost[0] = 15;
    ASSERT_EQ((fieldboost_t)15, boost[0]);

    boost[5] = 50;
    ASSERT_EQ((fieldboost_t)50, boost[5]);

    boost[7] = 80;
    ASSERT_EQ((fieldboost_t)80, boost[7]);
}

END_HA3_NAMESPACE(config);


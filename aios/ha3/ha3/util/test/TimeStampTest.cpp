#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/TimeStamp.h>

BEGIN_HA3_NAMESPACE(util);

class TimeStampTest : public TESTBASE {
public:
    TimeStampTest();
    ~TimeStampTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(service, TimeStampTest);


TimeStampTest::TimeStampTest() { 
}

TimeStampTest::~TimeStampTest() { 
}

void TimeStampTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void TimeStampTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(TimeStampTest, testSetCurrrentTimeStamp) { 
    HA3_LOG(DEBUG, "Begin Test!");

    TimeStamp timestamp1(0,100);
    TimeStamp timestamp2(0,200);
    
    ASSERT_TRUE((timestamp1 != timestamp2));
}

TEST_F(TimeStampTest, testMinusOperator) { 
    HA3_LOG(DEBUG, "Begin Test!");

    TimeStamp timestamp1(0,100);
    TimeStamp timestamp2(0,200);

    ASSERT_TRUE((timestamp2 - timestamp1) == (int64_t)100 );
}

TEST_F(TimeStampTest, testGetFormatTime) { 
    HA3_LOG(DEBUG, "Begin Test!");

    TimeStamp timestamp1(1307605837, 321000);
    ASSERT_EQ(std::string("2011-06-09 15:50:37.321"), 
                         timestamp1.getFormatTime());

    int64_t timeStamp = 1307605837LL * 1000LL * 1000LL + 321000;
    TimeStamp ts2(timeStamp);
    ASSERT_EQ(std::string("2011-06-09 15:50:37.321"), 
                         ts2.getFormatTime());

    ASSERT_EQ(std::string("2011-06-09 15:50:37.321"), 
                         TimeStamp::getFormatTime(timeStamp));
}


END_HA3_NAMESPACE(util);


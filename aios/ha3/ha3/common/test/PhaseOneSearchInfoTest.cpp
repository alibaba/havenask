#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/PhaseOneSearchInfo.h>

BEGIN_HA3_NAMESPACE(common);

class PhaseOneSearchInfoTest : public TESTBASE {
public:
    PhaseOneSearchInfoTest();
    ~PhaseOneSearchInfoTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, PhaseOneSearchInfoTest);


#define ASSERT_SEARCH_INFO(first, second)       \
    ASSERT_TRUE(first == second);

PhaseOneSearchInfoTest::PhaseOneSearchInfoTest() { 
}

PhaseOneSearchInfoTest::~PhaseOneSearchInfoTest() { 
}

void PhaseOneSearchInfoTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void PhaseOneSearchInfoTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(PhaseOneSearchInfoTest, testSerialize) { 
    {
        PhaseOneSearchInfo searchInfo(1,2,3,4,5,6,7,8,9);
        autil::DataBuffer dataBuffer;
        dataBuffer.write(searchInfo);
        PhaseOneSearchInfo other;
        dataBuffer.read(other);
        ASSERT_SEARCH_INFO(searchInfo, other);
    }
    {
        PhaseOneSearchInfo searchInfo(1,2,3,4,5,6,7,8,9);
        searchInfo.otherInfoStr = "abc";
        searchInfo.qrsSearchInfo.appendQrsInfo("abcd");
        autil::DataBuffer dataBuffer;
        dataBuffer.write(searchInfo);
        PhaseOneSearchInfo other;
        dataBuffer.read(other);
        ASSERT_SEARCH_INFO(searchInfo, other);
        ASSERT_TRUE(other.otherInfoStr == "abc");
        ASSERT_TRUE(other.qrsSearchInfo.getQrsSearchInfo() == "abcd");
    }
}

TEST_F(PhaseOneSearchInfoTest, testMerge) {    
    PhaseOneSearchInfo searchInfo1(1,2,3,4,5,6,7,8,9);
    PhaseOneSearchInfo searchInfo2(11,12,13,14,15,16,17,18,19);
    PhaseOneSearchInfo searchInfo3(21,22,23,24,25,26,27,28,29);
    searchInfo1.otherInfoStr = "(a)";
    searchInfo2.otherInfoStr = "(b)";
    searchInfo3.otherInfoStr = "(c)";
    searchInfo1.qrsSearchInfo.appendQrsInfo("d");
    std::vector<PhaseOneSearchInfo*> vec;
    vec.push_back(&searchInfo1);
    vec.push_back(&searchInfo2);
    vec.push_back(&searchInfo3);

    PhaseOneSearchInfo result;
    result.mergeFrom(vec);
    PhaseOneSearchInfo expect(33, 36, 39, 42, 45, 48, 51, 54, 57);
    expect.otherInfoStr = "(a)(b)(c)";
    ASSERT_SEARCH_INFO(expect, result);
    ASSERT_TRUE(expect.qrsSearchInfo.getQrsSearchInfo() == "");
}

END_HA3_NAMESPACE(common);


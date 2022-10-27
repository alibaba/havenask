#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/util/RangeUtil.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(util);

class RangeUtilTest : public TESTBASE {
public:
    RangeUtilTest();
    ~RangeUtilTest();
public:
    void setUp();
    void tearDown();
protected:
    void checkSplitRange(uint32_t rangeFrom, uint32_t rangeTo, 
                         uint32_t partitionCount, 
                         const std::string &expectRangeStr);
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, RangeUtilTest);


RangeUtilTest::RangeUtilTest() { 
}

RangeUtilTest::~RangeUtilTest() { 
}

void RangeUtilTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void RangeUtilTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RangeUtilTest, testSplitRange) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    checkSplitRange(0, 999, 2, "0_499 500_999");
    checkSplitRange(0, 999, 3, "0_333 334_666 667_999");
    checkSplitRange(0, 0, 2, "0_0");
    checkSplitRange(0, 0, 1, "0_0");
    checkSplitRange(0, 1, 1, "0_1");
    checkSplitRange(0, 3, 0, "");
    checkSplitRange(0, 4, 3, "0_1 2_3 4_4");

    checkSplitRange(1, 3, 4, "1_1 2_2 3_3");
    checkSplitRange(1, 3, 3, "1_1 2_2 3_3");
    checkSplitRange(100, 199, 3, "100_133 134_166 167_199");
    checkSplitRange(1, 20, 3, "1_7 8_14 15_20");
    checkSplitRange(35533, 65535, 3, "35533_45533 45534_55534 55535_65535");
}

void RangeUtilTest::checkSplitRange(uint32_t rangeFrom,
                                    uint32_t rangeTo,
                                    uint32_t partitionCount, 
                                    const std::string &expectRangeStr) 
{
    HA3_LOG(DEBUG, "checkSplitRange, rangeTo = %u, partitionCount = %d",
            rangeTo, partitionCount);
    RangeVec rangeVec = 
        RangeUtil::splitRange(rangeFrom, rangeTo, partitionCount);
    string rangeStr;
    for (size_t i = 0; i < rangeVec.size(); ++i) {
        if (i > 0) {
            rangeStr += " ";
        }
        rangeStr += autil::StringUtil::toString(rangeVec[i].first) 
                    + "_" + autil::StringUtil::toString(rangeVec[i].second);
    }
    ASSERT_EQ(expectRangeStr, rangeStr);
}

END_HA3_NAMESPACE(util);


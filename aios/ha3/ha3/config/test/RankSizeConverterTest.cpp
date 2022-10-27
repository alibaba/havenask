#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/RankSizeConverter.h>
#include <ha3/util/NumericLimits.h>

BEGIN_HA3_NAMESPACE(config);

class RankSizeConverterTest : public TESTBASE {
public:
    RankSizeConverterTest();
    ~RankSizeConverterTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, RankSizeConverterTest);


RankSizeConverterTest::RankSizeConverterTest() { 
}

RankSizeConverterTest::~RankSizeConverterTest() { 
}

void RankSizeConverterTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void RankSizeConverterTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(RankSizeConverterTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    std::string num = "1000";
    ASSERT_EQ(uint32_t(1000), RankSizeConverter::convertToRankSize(num));

    num = " 1000 ";
    ASSERT_EQ(uint32_t(1000), RankSizeConverter::convertToRankSize(num));

    num = "";
    ASSERT_EQ(uint32_t(0), RankSizeConverter::convertToRankSize(num));

    num = "  ";
    ASSERT_EQ(uint32_t(0), RankSizeConverter::convertToRankSize(num));
}
TEST_F(RankSizeConverterTest, testUnlimited) {
    std::string num = "unlimited";
    ASSERT_EQ(util::NumericLimits<uint32_t>::max(), RankSizeConverter::convertToRankSize(num));

    num = "unlimiteD";
    ASSERT_EQ(util::NumericLimits<uint32_t>::max(), RankSizeConverter::convertToRankSize(num));

    num = "UNLIMITED";
    ASSERT_EQ(util::NumericLimits<uint32_t>::max(), RankSizeConverter::convertToRankSize(num));
}

TEST_F(RankSizeConverterTest, testInvalidNum) {
    std::string num = "-1";
    ASSERT_EQ(uint32_t(0), RankSizeConverter::convertToRankSize(num));

    num = "9999999999";
    ASSERT_EQ(uint32_t(0), RankSizeConverter::convertToRankSize(num));

    num = "a";
    ASSERT_EQ(uint32_t(0), RankSizeConverter::convertToRankSize(num));

    num = "100a";
    ASSERT_EQ(uint32_t(0), RankSizeConverter::convertToRankSize(num));
}
END_HA3_NAMESPACE(config);


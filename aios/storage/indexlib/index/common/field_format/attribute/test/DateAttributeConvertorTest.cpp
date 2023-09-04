#include "indexlib/index/common/field_format/attribute/DateAttributeConvertor.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/util/TimestampUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class DateAttributeConvertorTest : public TESTBASE
{
public:
    DateAttributeConvertorTest() = default;
    ~DateAttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void TestEncode(const std::string& value, uint32_t expectedValue);
};

TEST_F(DateAttributeConvertorTest, TestCaseForSimple)
{
    // 599616000000: 1989-01-01 00:00:00
    // 915148800000: 1999-01-01 00:00:00
    // 946684800000: 2000-01-01 00:00:00
    // 631152000000: 1990-01-01 00:00:00

    TestEncode("1989-01-01", 599616000000 / indexlib::util::TimestampUtil::DAY_MILLION_SEC);
    TestEncode("1999-01-01", 915148800000 / indexlib::util::TimestampUtil::DAY_MILLION_SEC);
    TestEncode("2000-01-01", 946684800000 / indexlib::util::TimestampUtil::DAY_MILLION_SEC);
    TestEncode("1990-01-01", 631152000000 / indexlib::util::TimestampUtil::DAY_MILLION_SEC);
}

void DateAttributeConvertorTest::TestEncode(const std::string& value, uint32_t expectedValue)
{
    DateAttributeConvertor convertor(false, "");
    autil::mem_pool::Pool pool;
    autil::StringView ret = convertor.Encode(autil::StringView(value), &pool);
    ASSERT_EQ(4, ret.length());
    uint32_t data = *((uint32_t*)ret.data());
    ASSERT_EQ(expectedValue, data);
}
} // namespace indexlibv2::index

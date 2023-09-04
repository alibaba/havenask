#include "indexlib/index/common/field_format/attribute/TimeAttributeConvertor.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/util/TimestampUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class TimeAttributeConvertorTest : public TESTBASE
{
public:
    TimeAttributeConvertorTest() = default;
    ~TimeAttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}

private:
    void TestEncode(const std::string& value, uint32_t expectedValue);
};

void TimeAttributeConvertorTest::TestEncode(const std::string& value, uint32_t expectedValue)
{
    TimeAttributeConvertor convertor(false, "");
    autil::mem_pool::Pool pool;
    autil::StringView ret = convertor.Encode(autil::StringView(value), &pool);
    ASSERT_EQ(4, ret.length());
    uint32_t data = *((uint32_t*)ret.data());
    ASSERT_EQ(expectedValue, data);
}

TEST_F(TimeAttributeConvertorTest, TestCaseForSimple)
{
    // 599616000000: 1989-01-01 00:00:00
    // 915148800000: 1999-01-01 00:00:00
    // 946684800000: 2000-01-01 00:00:00
    // 631152000000: 1990-01-01 00:00:00
    const uint32_t SECOND_MS = 1000;
    const uint32_t MINUTE_MS = 60 * SECOND_MS;
    const uint32_t HOUR_MS = 60 * MINUTE_MS;
    TestEncode("00:00:00", 0);
    TestEncode("00:10:00", 10 * MINUTE_MS);
    TestEncode("12:09:32.100", 12 * HOUR_MS + 9 * MINUTE_MS + 32 * SECOND_MS + 100);
    TestEncode("23:59:59.999", 23 * HOUR_MS + 59 * MINUTE_MS + 59 * SECOND_MS + 999);
}

} // namespace indexlibv2::index

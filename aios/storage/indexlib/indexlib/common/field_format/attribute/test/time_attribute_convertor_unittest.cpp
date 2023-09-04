#include "indexlib/common/field_format/attribute/test/time_attribute_convertor_unittest.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, TimeAttributeConvertorTest);

TimeAttributeConvertorTest::TimeAttributeConvertorTest() {}

TimeAttributeConvertorTest::~TimeAttributeConvertorTest() {}

void TimeAttributeConvertorTest::CaseSetUp() {}

void TimeAttributeConvertorTest::CaseTearDown() {}

void TimeAttributeConvertorTest::TestCaseForSimple()
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

void TimeAttributeConvertorTest::TestEncode(const string& value, uint32_t expectedValue)
{
    TimeAttributeConvertor convertor(false, "");
    autil::mem_pool::Pool pool;
    StringView ret = convertor.Encode(StringView(value), &pool);
    INDEXLIB_TEST_EQUAL(4, ret.length());
    uint32_t data = *((uint32_t*)ret.data());
    INDEXLIB_TEST_EQUAL(expectedValue, data);
}
}} // namespace indexlib::common

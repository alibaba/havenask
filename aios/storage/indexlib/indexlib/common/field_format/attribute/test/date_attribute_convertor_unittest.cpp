#include "indexlib/common/field_format/attribute/test/date_attribute_convertor_unittest.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
namespace indexlib { namespace common {
IE_LOG_SETUP(common, DateAttributeConvertorTest);

DateAttributeConvertorTest::DateAttributeConvertorTest() {}

DateAttributeConvertorTest::~DateAttributeConvertorTest() {}

void DateAttributeConvertorTest::CaseSetUp() {}

void DateAttributeConvertorTest::CaseTearDown() {}

void DateAttributeConvertorTest::TestCaseForSimple()
{
    // 599616000000: 1989-01-01 00:00:00
    // 915148800000: 1999-01-01 00:00:00
    // 946684800000: 2000-01-01 00:00:00
    // 631152000000: 1990-01-01 00:00:00

    TestEncode("1989-01-01", 599616000000 / TimestampUtil::DAY_MILLION_SEC);
    TestEncode("1999-01-01", 915148800000 / TimestampUtil::DAY_MILLION_SEC);
    TestEncode("2000-01-01", 946684800000 / TimestampUtil::DAY_MILLION_SEC);
    TestEncode("1990-01-01", 631152000000 / TimestampUtil::DAY_MILLION_SEC);
}

void DateAttributeConvertorTest::TestEncode(const string& value, uint32_t expectedValue)
{
    DateAttributeConvertor convertor(false, "");
    autil::mem_pool::Pool pool;
    StringView ret = convertor.Encode(StringView(value), &pool);
    INDEXLIB_TEST_EQUAL(4, ret.length());
    uint32_t data = *((uint32_t*)ret.data());
    INDEXLIB_TEST_EQUAL(expectedValue, data);
}
}} // namespace indexlib::common

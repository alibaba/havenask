#include "indexlib/common/field_format/attribute/test/compress_single_float_attribute_convertor_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/attribute/compress_single_float_attribute_convertor.h"
#include "indexlib/util/BlockFpEncoder.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, CompressSingleFloatAttributeConvertorTest);

CompressSingleFloatAttributeConvertorTest::CompressSingleFloatAttributeConvertorTest() {}

CompressSingleFloatAttributeConvertorTest::~CompressSingleFloatAttributeConvertorTest() {}

void CompressSingleFloatAttributeConvertorTest::CaseSetUp() {}

void CompressSingleFloatAttributeConvertorTest::CaseTearDown() {}

void CompressSingleFloatAttributeConvertorTest::TestSimpleProcess()
{
    // TODO add encode error test
    InnerTestEncode<int16_t>("fp16", 1.1);
    InnerTestEncode<int8_t>("int8#2", 1.3);
}
}} // namespace indexlib::common

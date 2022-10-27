#include "indexlib/common/field_format/attribute/test/compress_single_float_attribute_convertor_unittest.h"
#include "indexlib/common/field_format/attribute/compress_single_float_attribute_convertor.h"
#include "indexlib/util/block_fp_encoder.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, CompressSingleFloatAttributeConvertorTest);

CompressSingleFloatAttributeConvertorTest::CompressSingleFloatAttributeConvertorTest()
{
}

CompressSingleFloatAttributeConvertorTest::~CompressSingleFloatAttributeConvertorTest()
{
}

void CompressSingleFloatAttributeConvertorTest::CaseSetUp()
{
}

void CompressSingleFloatAttributeConvertorTest::CaseTearDown()
{
}


void CompressSingleFloatAttributeConvertorTest::TestSimpleProcess()
{
    //TODO add encode error test
    InnerTestEncode<int16_t>("fp16", 1.1);
    InnerTestEncode<int8_t>("int8#2", 1.3);
}

IE_NAMESPACE_END(common);


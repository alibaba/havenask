#pragma once

#include "indexlib/common/field_format/attribute/compress_float_attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class CompressFloatAttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    CompressFloatAttributeConvertorTest();
    ~CompressFloatAttributeConvertorTest();

    DECLARE_CLASS_NAME(CompressFloatAttributeConvertorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void InnerTestEncode(const std::vector<float>& data, bool needHash, int32_t fixedValueCount,
                         config::CompressTypeOption compressTypeOption);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressFloatAttributeConvertorTest, TestSimpleProcess);
}} // namespace indexlib::common

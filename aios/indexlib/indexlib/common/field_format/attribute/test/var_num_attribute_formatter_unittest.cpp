#include "indexlib/common/field_format/attribute/test/var_num_attribute_formatter_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, VarNumAttributeFormatterTest);

VarNumAttributeFormatterTest::VarNumAttributeFormatterTest()
{
}

VarNumAttributeFormatterTest::~VarNumAttributeFormatterTest()
{
}

void VarNumAttributeFormatterTest::CaseSetUp()
{
}

void VarNumAttributeFormatterTest::CaseTearDown()
{
}

void VarNumAttributeFormatterTest::TestSimpleProcess()
{
    InnerTestEncodeAndDecodeCount(0);
    InnerTestEncodeAndDecodeCount(12);
    InnerTestEncodeAndDecodeCount(63);
    InnerTestEncodeAndDecodeCount(64);
    InnerTestEncodeAndDecodeCount(255);
    InnerTestEncodeAndDecodeCount(16383);
    InnerTestEncodeAndDecodeCount(16384); 
    InnerTestEncodeAndDecodeCount(12351461);
    InnerTestEncodeAndDecodeCount(1073741823);

    // for (uint32_t i = 0; i < 1073741823; i++)
    // {
    //     InnerTestEncodeAndDecodeCount(i);
    // }
}

void VarNumAttributeFormatterTest::InnerTestEncodeAndDecodeCount(uint32_t countValue)
{
    char buffer[4] = {0};

    size_t encodeLen = VarNumAttributeFormatter::EncodeCount(countValue, buffer, 4);
    size_t decodeLen = 0;
    uint32_t decodeValue = VarNumAttributeFormatter::DecodeCount(buffer, decodeLen);
    ASSERT_EQ(decodeLen, encodeLen);
    ASSERT_EQ(decodeValue, countValue);
}

IE_NAMESPACE_END(common);


#ifndef __INDEXLIB_ATTRIBUTECONVERTORTEST_H
#define __INDEXLIB_ATTRIBUTECONVERTORTEST_H

#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace common {

class AttributeConvertorTest : public INDEXLIB_TESTBASE
{
public:
    class MockAttributeConvertor : public AttributeConvertor
    {
    public:
        MockAttributeConvertor() : AttributeConvertor(false, "") { mEncodeEmpty = true; }

        MOCK_METHOD(autil::StringView, EncodeFromAttrValueMeta,
                    (const AttrValueMeta& attrValueMeta, autil::mem_pool::Pool* memPool), (override));

        MOCK_METHOD(autil::StringView, EncodeFromRawIndexValue,
                    (const autil::StringView& rawValue, autil::mem_pool::Pool* memPool), (override));

        MOCK_METHOD(autil::StringView, InnerEncode,
                    (const autil::StringView& attrData, autil::mem_pool::Pool* memPool, std::string& strResult,
                     char* outBuffer, EncodeStatus& status),
                    (override));
        MOCK_METHOD(AttrValueMeta, Decode, (const autil::StringView& str), (override));
        MOCK_METHOD(std::string, EncodeNullValue, (), (override));
    };

public:
    AttributeConvertorTest();
    ~AttributeConvertorTest();

    DECLARE_CLASS_NAME(AttributeConvertorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAllocate();
    void TestEncode();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(AttributeConvertorTest, TestAllocate);
INDEXLIB_UNIT_TEST_CASE(AttributeConvertorTest, TestEncode);
}} // namespace indexlib::common

#endif //__INDEXLIB_ATTRIBUTECONVERTORTEST_H

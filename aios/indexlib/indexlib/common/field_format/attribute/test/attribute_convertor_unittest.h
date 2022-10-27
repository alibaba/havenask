#ifndef __INDEXLIB_ATTRIBUTECONVERTORTEST_H
#define __INDEXLIB_ATTRIBUTECONVERTORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"

IE_NAMESPACE_BEGIN(common);

class AttributeConvertorTest : public INDEXLIB_TESTBASE {
public:
    class MockAttributeConvertor : public AttributeConvertor
    {
    public:
        MockAttributeConvertor()
            : AttributeConvertor(false, "")
        { mEncodeEmpty = true; }

        MOCK_METHOD2(EncodeFromAttrValueMeta,
                     autil::ConstString(const AttrValueMeta& attrValueMeta, autil::mem_pool::Pool *memPool));
        
        MOCK_METHOD2(EncodeFromRawIndexValue,
                     autil::ConstString(const autil::ConstString& rawValue, autil::mem_pool::Pool *memPool));

        MOCK_METHOD5(InnerEncode, autil::ConstString(
                         const autil::ConstString &attrData,
                         autil::mem_pool::Pool *memPool, 
                         std::string &strResult, char* outBuffer, EncodeStatus &status));
        MOCK_METHOD1(Decode, AttrValueMeta(const autil::ConstString& str));
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

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTECONVERTORTEST_H

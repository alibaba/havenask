#include "indexlib/common/field_format/attribute/test/attribute_convertor_unittest.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, AttributeConvertorTest);

AttributeConvertorTest::AttributeConvertorTest()
{
}

AttributeConvertorTest::~AttributeConvertorTest()
{
}

void AttributeConvertorTest::CaseSetUp()
{
}

void AttributeConvertorTest::CaseTearDown()
{
}

void AttributeConvertorTest::TestAllocate()
{
    autil::mem_pool::Pool pool;
    char buffer[8];
    string str;

    MockAttributeConvertor attrConv;
    INDEXLIB_TEST_EQUAL(buffer, attrConv.allocate(
                    &pool, str, buffer, 8));

    char *addr = attrConv.allocate(NULL, str, NULL, 8);
    INDEXLIB_TEST_EQUAL(str.data(), addr);

    addr = attrConv.allocate(&pool, str, NULL, 8);
    INDEXLIB_TEST_TRUE(addr != NULL);
    INDEXLIB_TEST_TRUE(addr != buffer);
    INDEXLIB_TEST_TRUE(addr != str.data());
    INDEXLIB_TEST_EQUAL((size_t)8, pool.getAllocatedSize());
}

void AttributeConvertorTest::TestEncode()
{
    MockAttributeConvertor attrConv;
    ConstString attrData;
    autil::mem_pool::Pool pool;
    string str;
    char buffer[8];

    EXPECT_CALL(attrConv, InnerEncode(attrData,&pool,_,NULL,_))
        .WillOnce(Return(attrData));
    attrConv.Encode(attrData, &pool);

    EXPECT_CALL(attrConv, InnerEncode(ConstString(str),NULL,_,NULL,_))
        .WillOnce(Return(attrData));
    attrConv.Encode(str);

    EXPECT_CALL(attrConv, InnerEncode(attrData,NULL,_,buffer,_))
        .WillOnce(Return(attrData));
    attrConv.Encode(attrData, buffer);
}

IE_NAMESPACE_END(common);


#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"

#include "unittest/unittest.h"

namespace indexlibv2::index {

class MockAttributeConvertor : public AttributeConvertor
{
public:
    MockAttributeConvertor() : AttributeConvertor(false, "") { _encodeEmpty = true; }

    MOCK_METHOD(autil::StringView, EncodeFromAttrValueMeta,
                (const AttrValueMeta& attrValueMeta, autil::mem_pool::Pool* memPool), (override));

    MOCK_METHOD(autil::StringView, EncodeFromRawIndexValue,
                (const autil::StringView& rawValue, autil::mem_pool::Pool* memPool), (override));

    MOCK_METHOD(autil::StringView, InnerEncode,
                (const autil::StringView& attrData, autil::mem_pool::Pool* memPool, std::string& strResult,
                 char* outBuffer, EncodeStatus& status),
                (override));
    MOCK_METHOD(AttrValueMeta, Decode, (const autil::StringView& str), (override));
    MOCK_METHOD((std::pair<Status, std::string>), EncodeNullValue, (), (override));
};

class AttributeConvertorTest : public TESTBASE
{
public:
    AttributeConvertorTest() = default;
    ~AttributeConvertorTest() = default;
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(AttributeConvertorTest, TestAllocate)
{
    autil::mem_pool::Pool pool;
    char buffer[8];
    std::string str;

    MockAttributeConvertor attrConv;
    ASSERT_EQ(buffer, attrConv.Allocate(&pool, str, buffer, 8));

    char* addr = attrConv.Allocate(NULL, str, NULL, 8);
    ASSERT_EQ(str.data(), addr);

    addr = attrConv.Allocate(&pool, str, NULL, 8);
    ASSERT_TRUE(addr != NULL);
    ASSERT_TRUE(addr != buffer);
    ASSERT_TRUE(addr != str.data());
    ASSERT_EQ((size_t)8, pool.getAllocatedSize());
}

TEST_F(AttributeConvertorTest, TestEncode)
{
    MockAttributeConvertor attrConv;
    autil::StringView attrData;
    autil::mem_pool::Pool pool;
    std::string str;
    char buffer[8];

    EXPECT_CALL(attrConv, InnerEncode(attrData, &pool, _, NULL, _)).WillOnce(Return(attrData));
    attrConv.Encode(attrData, &pool);

    EXPECT_CALL(attrConv, InnerEncode(autil::StringView(str), NULL, _, NULL, _)).WillOnce(Return(attrData));
    attrConv.Encode(str);

    EXPECT_CALL(attrConv, InnerEncode(attrData, NULL, _, buffer, _)).WillOnce(Return(attrData));
    attrConv.Encode(attrData, buffer);
}
} // namespace indexlibv2::index

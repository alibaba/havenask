#include "indexlib/index/attribute/MultiValueAttributeMemReader.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {
class MultiValueAttributeMemReaderTest : public TESTBASE
{
public:
    MultiValueAttributeMemReaderTest() = default;
    ~MultiValueAttributeMemReaderTest() = default;

    void setUp() override;
    void tearDown() override {}

private:
    autil::mem_pool::Pool _pool;
    std::unique_ptr<VarLenDataAccessor> _varLenDataAccessor;
};

void MultiValueAttributeMemReaderTest::setUp()
{
    _varLenDataAccessor = std::make_unique<VarLenDataAccessor>();
    _varLenDataAccessor->Init(&_pool, false);
}

TEST_F(MultiValueAttributeMemReaderTest, TestRead)
{
    char buffer[16];
    MultiValueAttributeFormatter::EncodeCount(3, buffer, 16);
    memcpy(buffer + 1, "123", 3);

    autil::StringView encodeValue(buffer, 4);
    _varLenDataAccessor->AppendValue(encodeValue);

    indexlib::config::CompressTypeOption cmpOption;
    MultiValueAttributeMemReader<char> reader(_varLenDataAccessor.get(), cmpOption, -1, false);
    autil::MultiValueType<char> value;
    bool isNull;
    ASSERT_TRUE(reader.Read((docid_t)0, value, isNull, /* pool */ nullptr));
    ASSERT_EQ((uint32_t)3, value.size());
    ASSERT_EQ(std::string("123"), std::string(value.data(), value.size()));
    ASSERT_FALSE(isNull);
    ASSERT_FALSE(value.isNull());
    ASSERT_FALSE(reader.Read((docid_t)1, value, isNull, /* pool */ nullptr));
}

TEST_F(MultiValueAttributeMemReaderTest, TestReadNull)
{
    char buffer[16];
    size_t len = MultiValueAttributeFormatter::EncodeCount(
        MultiValueAttributeFormatter::MULTI_VALUE_NULL_FIELD_VALUE_COUNT, buffer, 16);

    autil::StringView encodeValue(buffer, len);
    _varLenDataAccessor->AppendValue(encodeValue);

    indexlib::config::CompressTypeOption cmpOption;
    MultiValueAttributeMemReader<char> reader(_varLenDataAccessor.get(), cmpOption, -1, true);
    autil::MultiValueType<char> value;
    bool isNull;
    ASSERT_TRUE(reader.Read((docid_t)0, value, isNull, /* pool */ nullptr));
    ASSERT_EQ((uint32_t)0, value.size());
    ASSERT_TRUE(isNull);
    ASSERT_TRUE(value.isNull());

    autil::MultiValueType<char> cValue;
    ASSERT_TRUE(reader.Read((docid_t)0, cValue, isNull, /* pool */ nullptr));
    ASSERT_EQ((uint32_t)0, cValue.size());
    ASSERT_TRUE(isNull);
    ASSERT_TRUE(cValue.isNull());
}
} // namespace indexlibv2::index

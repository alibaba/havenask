#include "indexlib/index/normal/attribute/test/in_mem_var_num_attribute_reader_unittest.h"
#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(attribute, InMemVarNumAttributeReaderTest);

InMemVarNumAttributeReaderTest::InMemVarNumAttributeReaderTest()
{
}

InMemVarNumAttributeReaderTest::~InMemVarNumAttributeReaderTest()
{
}

void InMemVarNumAttributeReaderTest::CaseSetUp()
{
}

void InMemVarNumAttributeReaderTest::CaseTearDown()
{
}

void InMemVarNumAttributeReaderTest::TestRead()
{
    VarNumAttributeAccessor accessor;
    accessor.Init(&mPool);

    char buffer[16];
    VarNumAttributeFormatter::EncodeCount(3, buffer, 16);
    memcpy(buffer + 1, "123", 3);

    ConstString encodeValue(buffer, 4);
    AttrValueMeta attrMeta(1, encodeValue);
    accessor.AddNormalField(attrMeta);

    config::CompressTypeOption cmpOption;
    InMemVarNumAttributeReader<char> reader(&accessor, cmpOption, -1);
    MultiValueType<char> value;
    ASSERT_TRUE(reader.Read((docid_t)0, value));
    ASSERT_EQ((uint32_t)3, value.size());
    ASSERT_EQ(string("123"), string(value.data(), value.size()));

    ASSERT_FALSE(reader.Read((docid_t)1, value));
}

IE_NAMESPACE_END(index);


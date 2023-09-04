#include "indexlib/index/kv/FieldValueExtractor.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/test/KVDocumentBatchMaker.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::document;
using namespace indexlib::config;

namespace indexlibv2 { namespace index {

class FieldValueExtractorTest : public TESTBASE
{
public:
    FieldValueExtractorTest();
    ~FieldValueExtractorTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestPackValue();
    void TestSimpleValue();

private:
    autil::mem_pool::Pool _pool;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, FieldValueExtractorTest);

FieldValueExtractorTest::FieldValueExtractorTest() {}

FieldValueExtractorTest::~FieldValueExtractorTest() {}

TEST_F(FieldValueExtractorTest, TestSimpleValue)
{
    string field = "int8_single:int8;int16_single:int16;int32_single:int32;int64_single:int64;"
                   "int8_multi:int8:true;int16_multi:int16:true;int32_multi:int32:true;int64_multi:int64:true;"
                   "float_single:float;str_single:string;str_multi:string:true";
    string keyStr = "str_single";
    string valueStr = "int32_single";
    auto [schema, kvConfig] = KVIndexConfigBuilder::MakeIndexConfig(field, keyStr, valueStr);
    ASSERT_TRUE(schema);
    ASSERT_TRUE(kvConfig);
    auto [status, packAttrConfig] = kvConfig->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(packAttrConfig);

    PackAttributeFormatter packFormatter;
    packFormatter.Init(packAttrConfig);

    int32_t value = 32;
    auto packValue = autil::StringView((const char*)&value, sizeof(value));
    FieldValueExtractor extractor(&packFormatter, packValue, &_pool);
    ASSERT_EQ(1, extractor.GetFieldCount());

    size_t idx;
    FieldType type;
    bool isMulti;
    ASSERT_TRUE(extractor.GetValueMeta("int32_single", idx, type, isMulti));
    ASSERT_EQ(0, idx);
    ASSERT_EQ(ft_int32, type);
    ASSERT_FALSE(isMulti);

    int32_t rValue;
    ASSERT_TRUE(extractor.GetTypedValue("int32_single", rValue));
    ASSERT_EQ(value, rValue);

    ASSERT_TRUE(extractor.GetTypedValue(0, rValue));
    ASSERT_EQ(value, rValue);

    autil::MultiInt32 multiValue;
    ASSERT_FALSE(extractor.GetTypedValue(0, multiValue));

    std::string strValue;
    ASSERT_TRUE(extractor.GetStringValue(0, strValue));
    ASSERT_EQ("32", strValue);
}

TEST_F(FieldValueExtractorTest, TestPackValue)
{
    string field = "int8_single:int8;int16_single:int16;int32_single:int32;int64_single:int64;"
                   "int8_multi:int8:true;int16_multi:int16:true;int32_multi:int32:true;int64_multi:int64:true;"
                   "float_single:float;str_single:string;str_multi:string:true";
    string key = "str_single";

    string value = "int32_single;float_single;int16_multi;str_single;str_multi";
    auto [schema, kvConfig] = KVIndexConfigBuilder::MakeIndexConfig(field, key, value);
    ASSERT_TRUE(schema);
    ASSERT_TRUE(kvConfig);
    string docStr = "cmd=add,int32_single=32,float_single=11,int16_multi=1 2 3,str_single=abc,str_multi=efg hij";
    auto docBatch = document::KVDocumentBatchMaker::Make(schema, docStr);
    ASSERT_TRUE(docBatch);
    ASSERT_EQ(1, docBatch->GetBatchSize());
    auto doc = dynamic_pointer_cast<document::KVDocument>((*docBatch)[0]);
    ASSERT_TRUE(doc);

    auto [status, packAttrConfig] = kvConfig->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(packAttrConfig);
    PackAttributeFormatter packFormatter;
    packFormatter.Init(packAttrConfig);

    autil::StringView packedField = doc->GetValue();
    auto packDataAttrConfig = packAttrConfig->CreateAttributeConfig();
    ASSERT_TRUE(packDataAttrConfig);
    std::shared_ptr<indexlibv2::index::AttributeConvertor> convertor(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));
    autil::StringView encodePackData = convertor->Decode(packedField).data;
    size_t countLen = 0;
    autil::MultiValueFormatter::decodeCount(encodePackData.data(), countLen);
    const char* data = encodePackData.data() + countLen;
    auto packValue = autil::StringView(data, encodePackData.size() - countLen);

    FieldValueExtractor extractor(&packFormatter, packValue, &_pool);
    ASSERT_EQ(5, extractor.GetFieldCount());

    auto checkValueMeta = [&](size_t idx, string name, FieldType type, bool isMulti) {
        string name_;
        FieldType type_;
        bool isMulti_;
        ASSERT_TRUE(extractor.GetValueMeta(idx, name_, type_, isMulti_));
        ASSERT_EQ(name, name_);
        ASSERT_EQ(type, type_);
        ASSERT_EQ(isMulti, isMulti_);
    };

    checkValueMeta(0, "int32_single", ft_int32, false);
    checkValueMeta(1, "float_single", ft_float, false);
    checkValueMeta(2, "int16_multi", ft_int16, true);
    checkValueMeta(3, "str_single", ft_string, false);
    checkValueMeta(4, "str_multi", ft_string, true);

    int32_t i32;
    int8_t i8;
    autil::MultiChar str;
    autil::MultiString multiStr;

    ASSERT_TRUE(extractor.GetTypedValue("int32_single", i32));
    ASSERT_EQ(32, i32);
    ASSERT_FALSE(extractor.GetTypedValue("int32_single", i8));
    ASSERT_TRUE(extractor.GetTypedValue("str_single", str));
    ASSERT_EQ(std::string("abc"), std::string(str.getData(), str.getDataSize()));

    ASSERT_FALSE(extractor.GetTypedValue("str_single", multiStr));
    ASSERT_TRUE(extractor.GetTypedValue("str_multi", multiStr));
    ASSERT_EQ(2, multiStr.size());
}

}} // namespace indexlibv2::index

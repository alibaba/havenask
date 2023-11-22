#include "indexlib/common/field_format/pack_attribute/test/pack_attribute_formatter_unittest.h"

#include "autil/MultiValueType.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/test/document_creator.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::document;
using namespace indexlib::util;
using namespace indexlib::misc;

namespace indexlib { namespace common {
IE_LOG_SETUP(common, PackAttributeFormatterTest);

PackAttributeFormatterTest::PackAttributeFormatterTest() : mPool(new Pool()) {}

PackAttributeFormatterTest::~PackAttributeFormatterTest() { DELETE_AND_SET_NULL(mPool); }

void PackAttributeFormatterTest::CaseSetUp()
{
    mSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_with_all_type_pack_attributes.json");
    UpdateImpactValue(mSchema);
    mDocStr = "cmd=add,"
              "int8_single=0,int8_multi=0 1,"
              "int16_single=1,int16_multi=1 2,"
              "int32_single=2,int32_multi=2 3,"
              "int64_single=3,int64_multi=3 4,"
              "uint8_single=5,uint8_multi=5 6,"
              "uint16_single=6,uint16_multi=6 7,"
              "uint32_single=7,uint32_multi=7 8,"
              "uint64_single=8,uint64_multi=8 9,"
              "float_single=9,float_multi=9 10,"
              "double_single=10,double_multi=10 11,"
              "str_single=abc,str_multi=abc def,"
              "int8_single_nopack=23,str_multi_nopack=i don't pack";

    mDoc = DocumentCreator::CreateNormalDocument(mSchema, mDocStr);
    assert(mDoc->GetAttributeDocument()->GetPackFieldCount() == 0);
}

void PackAttributeFormatterTest::CaseTearDown() {}

void PackAttributeFormatterTest::TestSimpleProcess()
{
    PackAttributeConfigPtr packConfig = mSchema->GetAttributeSchema()->GetPackAttributeConfig(0);
    PackAttributeFormatter packFormatter;
    packFormatter.Init(packConfig);

    StringView packedField = packFormatter.Format(GetFields(&packFormatter, mDoc), mDoc->GetPool());

    CheckReference(packFormatter, packConfig, packedField, "0,0 1,1,1 2,2,2 3,3,3 4,9,9 10,abc");

    PackAttributeConfigPtr uniqPackConfig = mSchema->GetAttributeSchema()->GetPackAttributeConfig(1);
    PackAttributeFormatter uniqPackFormatter;
    uniqPackFormatter.Init(uniqPackConfig);

    packedField = uniqPackFormatter.Format(GetFields(&uniqPackFormatter, mDoc), mDoc->GetPool());
    CheckReference(uniqPackFormatter, uniqPackConfig, packedField, "5,5 6,6,6 7,7,7 8,8,8 9,10,10 11,abc def");
}

void PackAttributeFormatterTest::TestMergeAndFormatUpdateFields()
{
    InnerTestMergeAndFormatUpdateFields(0, "1:012;2:200;10:abcde", true, "0,0 1 2,200,1 2,2,2 3,3,3 4,9,9 10,abcde");

    InnerTestMergeAndFormatUpdateFields(0, "1:012;2:200;10:abcde", false, "0,0 1 2,200,1 2,2,2 3,3,3 4,9,9 10,abcde");

    InnerTestMergeAndFormatUpdateFields(1, "11:51;12:567;20:101112;21:abcdefghi", true,
                                        "51,5 6 7,6,6 7,7,7 8,8,8 9,10,10 11 12,abc def ghi");

    InnerTestMergeAndFormatUpdateFields(1, "11:51;12:567;20:101112;21:abcdefghi", false,
                                        "51,5 6 7,6,6 7,7,7 8,8,8 9,10,10 11 12,abc def ghi");
}

void PackAttributeFormatterTest::TestEncodeAndDecodePatchValues()
{
    PackAttributeFields updateFields;
    ConstructUpdateFields("1:012;2:200;10:abcde", false, updateFields);
    uint8_t buffer[256 * 1024];

    // test null buffer
    ASSERT_EQ((size_t)0, PackAttributeFormatter::EncodePatchValues(updateFields, NULL, 10));

    // test buffer is not enough
    size_t encodeSize = PackAttributeFormatter::EncodePatchValues(updateFields, buffer, 10);
    ASSERT_EQ((size_t)0, encodeSize);

    // test normal case
    encodeSize = PackAttributeFormatter::EncodePatchValues(updateFields, buffer, 256 * 1024);
    ASSERT_TRUE(encodeSize > 0);

    PackAttributeFields decodeFields;
    ASSERT_FALSE(PackAttributeFormatter::DecodePatchValues(buffer, encodeSize - 4, decodeFields));

    decodeFields.clear();
    ASSERT_FALSE(PackAttributeFormatter::DecodePatchValues(buffer, encodeSize + 1, decodeFields));

    decodeFields.clear();
    ASSERT_TRUE(PackAttributeFormatter::DecodePatchValues(buffer, encodeSize, decodeFields));

    ASSERT_EQ(updateFields.size(), decodeFields.size());
    for (size_t i = 0; i < updateFields.size(); ++i) {
        EXPECT_EQ(updateFields[i].first, decodeFields[i].first);
        EXPECT_EQ(updateFields[i].second, decodeFields[i].second);
    }
}

void PackAttributeFormatterTest::TestTotalErrorCountWithDefaultValue()
{
    ErrorLogCollector::EnableErrorLogCollect();
    ErrorLogCollector::EnableTotalErrorCount();
    ErrorLogCollector::ResetTotalErrorCount();

    PackAttributeConfigPtr packConfig = mSchema->GetAttributeSchema()->GetPackAttributeConfig(0);
    PackAttributeFormatter packFormatter;
    packFormatter.Init(packConfig);

    ASSERT_EQ((int64_t)0, ErrorLogCollector::GetTotalErrorCount());
}

void PackAttributeFormatterTest::InnerTestMergeAndFormatUpdateFields(bool packAttrId, const string& updateFieldsStr,
                                                                     bool hasHashKeyInUpdateFields,
                                                                     const string& expectValueStr)
{
    PackAttributeConfigPtr packConfig = mSchema->GetAttributeSchema()->GetPackAttributeConfig(packAttrId);
    PackAttributeFormatter packFormatter;
    packFormatter.Init(packConfig);

    StringView oldValue = packFormatter.Format(GetFields(&packFormatter, mDoc), mDoc->GetPool());
    AttributeConfigPtr packDataAttrConfig = packConfig->CreateAttributeConfig();
    AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));

    // use multi_value_type getData
    StringView encodePackData = convertor->Decode(oldValue).data;
    size_t encodeCountLen = 0;
    MultiValueFormatter::decodeCount(encodePackData.data(), encodeCountLen);
    StringView packData = StringView(encodePackData.data() + encodeCountLen, encodePackData.size() - encodeCountLen);

    PackAttributeFields updateFields;
    ConstructUpdateFields(updateFieldsStr, hasHashKeyInUpdateFields, updateFields);

    MemBuffer buffer(256 * 1024);
    StringView mergeValue =
        packFormatter.MergeAndFormatUpdateFields(packData.data(), updateFields, hasHashKeyInUpdateFields, buffer);
    CheckReference(packFormatter, packConfig, mergeValue, expectValueStr);
}

void PackAttributeFormatterTest::TestCompactPackAttributeFormat()
{
    {
        // total length is fixed
        IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_schema/",
                                                                  "kv_index_schema_with_compact_pack2.json");
        UpdateImpactValue(schema);
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        ASSERT_TRUE(kvConfig.get() != NULL);
        const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        valueConfig->EnableCompactFormat(true);
        ASSERT_TRUE(valueConfig.get() != NULL);

        config::PackAttributeConfigPtr packAttrConfig = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(packAttrConfig.get() != NULL);

        string docStr = "cmd=add,"
                        "nid=33,multi_int32_fixed=1 2 3 4,int8=101,multi_float_fixed=11.2 99.99 1000.1";

        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr);
        PackAttributeFormatter packFormatter;
        packFormatter.Init(packAttrConfig);
        StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());
        CheckReference(packFormatter, packAttrConfig, packedField, "33,1 2 3 4,101,11.2 99.99 1000.1");
    }

    {
        IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_schema/",
                                                                  "kv_index_schema_with_compact_pack.json");
        UpdateImpactValue(schema);
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        ASSERT_TRUE(kvConfig.get() != NULL);
        const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        ASSERT_TRUE(valueConfig.get() != NULL);

        config::PackAttributeConfigPtr packAttrConfig = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(packAttrConfig.get() != NULL);
        string docStr = "cmd=add,"
                        "nid=33,multi_int32_fixed=9 10 11 12,multi_int16=1 2 3 4 5";

        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr);
        PackAttributeFormatter packFormatter;
        packFormatter.Init(packAttrConfig);

        StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());
        CheckReference(packFormatter, packAttrConfig, packedField, "33,9 10 11 12,1 2 3 4 5");
    }
}

void PackAttributeFormatterTest::TestFloatEncode()
{
    // test blockfp encode & fp16 encode
    {
        IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_schema/",
                                                                  "kv_index_schema_with_compact_pack3.json");
        UpdateImpactValue(schema);
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        ASSERT_TRUE(kvConfig.get() != NULL);
        const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        valueConfig->EnableCompactFormat(true);
        ASSERT_TRUE(valueConfig.get() != NULL);

        config::PackAttributeConfigPtr packAttrConfig = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(packAttrConfig.get() != NULL);

        string docStr = "cmd=add,"
                        "nid=33,float_block_fp=0.1 0.2 -0.03 0.4,int8=101,float_fp16=0.2 0.399 0.041";

        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr);
        PackAttributeFormatter packFormatter;
        packFormatter.Init(packAttrConfig);
        ASSERT_EQ(34, packFormatter.mFixAttrSize);
        StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());
        CheckReference(packFormatter, packAttrConfig, packedField,
                       "33,0.1 0.2 -0.03 0.4,101,0.2 0.399 0.041 0 0 0 0 0");
    }
    // test single float compress
    {
        IndexPartitionSchemaPtr schema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_schema/",
                                                                  "kv_index_schema_with_compact_pack4.json");
        UpdateImpactValue(schema);
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
        ASSERT_TRUE(kvConfig.get() != NULL);
        const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        valueConfig->EnableCompactFormat(false);
        ASSERT_TRUE(valueConfig.get() != NULL);

        config::PackAttributeConfigPtr packAttrConfig = valueConfig->CreatePackAttributeConfig();
        ASSERT_TRUE(packAttrConfig.get() != NULL);

        string docStr = "cmd=add,"
                        "nid=33,float_int8=0.1,int8=101,float_fp16=9.1,multi_float_fp16=0.2 0.399 0.041";

        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, docStr);
        PackAttributeFormatter packFormatter;
        packFormatter.Init(packAttrConfig);
        ASSERT_EQ(20, packFormatter.mFixAttrSize);
        StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());
        CheckReference(packFormatter, packAttrConfig, packedField, "33,0.1,101,9.1,0.2 0.399 0.041 0", 0.01);
    }
}

void PackAttributeFormatterTest::TestEmptyMultiString()
{
    mSchema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_with_multi_string_pack_attr.json");
    UpdateImpactValue(mSchema);

    PackAttributeConfigPtr packConfig = mSchema->GetAttributeSchema()->GetPackAttributeConfig(0);
    PackAttributeFormatter packFormatter;
    packFormatter.Init(packConfig);

    string docStr = "cmd=add,int32_single=2,str_single=abc,str_multi=";
    document::NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, docStr);
    StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());

    AttributeConfigPtr packDataAttrConfig = packConfig->CreateAttributeConfig();
    AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));

    // use multi_value_type getData
    StringView encodePackData = convertor->Decode(packedField).data;
    size_t encodeCountLen = 0;
    MultiValueFormatter::decodeCount(encodePackData.data(), encodeCountLen);
    StringView packData = StringView(encodePackData.data() + encodeCountLen, encodePackData.size() - encodeCountLen);

    if (GET_CASE_PARAM()) {
        // sizeof(int32_t) + 1 byte(count = 0)
        ASSERT_EQ(5, packData.size());
    } else {
        // sizeof(int32_t) + sizeof(uint64_t) + 1 byte(count = 0)
        ASSERT_EQ(13, packData.size());
    }

    // update single_int32, reuse empty multi_str
    PackAttributeFields updateFields;
    ConstructUpdateFields("0:3", true, updateFields);

    MemBuffer buffer(256 * 1024);
    StringView mergeValue = packFormatter.MergeAndFormatUpdateFields(packData.data(), updateFields, true, buffer);
    encodePackData = convertor->Decode(mergeValue).data;
    MultiValueFormatter::decodeCount(encodePackData.data(), encodeCountLen);
    StringView mergedPackData =
        StringView(encodePackData.data() + encodeCountLen, encodePackData.size() - encodeCountLen);

    if (GET_CASE_PARAM()) {
        ASSERT_EQ(5, mergedPackData.size());
    } else {
        ASSERT_EQ(13, mergedPackData.size());
    }
}

void PackAttributeFormatterTest::TestCreatePlainFormatEncoder()
{
    auto schema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_with_all_type_pack_attributes.json");
    string docStr = "cmd=add,"
                    "int8_single=0,int8_multi=0 1,"
                    "int16_single=1,int16_multi=1 2,"
                    "int32_single=2,int32_multi=2 3,"
                    "int64_single=3,int64_multi=3 4,"
                    "uint8_single=5,uint8_multi=5 6,"
                    "uint16_single=6,uint16_multi=6 7,"
                    "uint32_single=7,uint32_multi=7 8,"
                    "uint64_single=8,uint64_multi=8 9,"
                    "float_single=9,float_multi=9 10,"
                    "double_single=10,double_multi=10 11,"
                    "str_single=abc,str_multi=abc def,"
                    "int8_single_nopack=23,str_multi_nopack=i don't pack";
    auto doc = DocumentCreator::CreateNormalDocument(schema, docStr);

    PackAttributeConfigPtr packConfig = schema->GetAttributeSchema()->GetPackAttributeConfig(0);
    PackAttributeFormatter packFormatter;
    packFormatter.Init(packConfig);

    StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());
    ASSERT_TRUE(packedField != StringView::empty_instance());
    auto CheckPlainFormat = [&](const PackAttributeFormatter& formatter, const PackAttributeConfigPtr& packConfig,
                                const StringView& packedField) {
        ASSERT_TRUE(packedField != StringView::empty_instance());

        AttributeConfigPtr packDataAttrConfig = packConfig->CreateAttributeConfig();
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));
        StringView encodePackData = convertor->Decode(packedField).data;
        unique_ptr<PlainFormatEncoder> plainEncoder(formatter.CreatePlainFormatEncoder());
        ASSERT_TRUE(plainEncoder);
        StringView plainValue;
        char buffer[encodePackData.size()];
        ASSERT_TRUE(plainEncoder->Encode(encodePackData, buffer, encodePackData.size(), plainValue));

        size_t encodeCountLen = 0;
        MultiValueFormatter::decodeCount(plainValue.data(), encodeCountLen);
        StringView plainPackData = StringView(plainValue.data() + encodeCountLen, plainValue.size() - encodeCountLen);
        StringView packData;
        ASSERT_TRUE(plainEncoder->Decode(plainPackData, doc->GetPool(), packData));

        MultiValueFormatter::decodeCount(encodePackData.data(), encodeCountLen);
        StringView expectPackData =
            StringView(encodePackData.data() + encodeCountLen, encodePackData.size() - encodeCountLen);

        ASSERT_EQ(expectPackData.size(), packData.size());
        ASSERT_EQ(0, memcmp(expectPackData.data(), packData.data(), packData.size()));
    };

    CheckPlainFormat(packFormatter, packConfig, packedField);

    PackAttributeConfigPtr uniqPackConfig = schema->GetAttributeSchema()->GetPackAttributeConfig(1);
    PackAttributeFormatter uniqPackFormatter;
    uniqPackFormatter.Init(uniqPackConfig);
    packedField = uniqPackFormatter.Format(GetFields(&uniqPackFormatter, doc), doc->GetPool());
    CheckPlainFormat(uniqPackFormatter, uniqPackConfig, packedField);

    schema = SchemaLoader::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), "schema_with_multi_string_pack_attr.json");
    packConfig = schema->GetAttributeSchema()->GetPackAttributeConfig(0);
    PackAttributeFormatter multiStrPackFormatter;
    multiStrPackFormatter.Init(packConfig);

    // test empty multi string
    docStr = "cmd=add,int32_single=2,str_single=abc,str_multi=";
    doc = DocumentCreator::CreateNormalDocument(schema, docStr);
    packedField = multiStrPackFormatter.Format(GetFields(&multiStrPackFormatter, doc), doc->GetPool());
    ASSERT_TRUE(packedField != StringView::empty_instance());
    CheckPlainFormat(multiStrPackFormatter, packConfig, packedField);
}

void PackAttributeFormatterTest::TestEnableImpactFormat()
{
    string field = "int8_single:int8;int16_single:int16;int32_single:int32;int64_single:int64;"
                   "int8_multi:int8:true;int16_multi:int16:true;int32_multi:int32:true;int64_multi:int64:true;"
                   "float_single:float;str_single:string;str_multi:string:true";
    string index = "pk:primarykey64:str_single";

    auto CheckVarLenReference = [](PackAttributeConfigPtr packAttrConfig, const char* attrName, size_t offset,
                                   bool needHeader, uint64_t totalVarNum, uint64_t varIdx) {
        PackAttributeFormatter packFormatter;
        packFormatter.Init(packAttrConfig);
        ASSERT_EQ(offset, packFormatter.mFixAttrSize);

        auto ref = packFormatter.GetAttributeReference(attrName);
        ASSERT_TRUE(ref);
        auto refOffset = ref->GetOffset();
        ASSERT_TRUE(refOffset.isImpactFormat());
        ASSERT_EQ(offset, refOffset.getOffset());
        ASSERT_EQ(needHeader, refOffset.needVarLenHeader());
        ASSERT_EQ(totalVarNum, refOffset.getVarLenFieldNum());
        ASSERT_EQ(varIdx, refOffset.getVarLenFieldIdx());
    };

    {
        // test only single var field, no need adaptive offset
        string attribute = "pack_attr:int32_single,float_single,int32_multi";
        auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
        PackAttributeConfigPtr packAttrConfig = schema->GetAttributeSchema()->GetPackAttributeConfig("pack_attr");
        ASSERT_TRUE(packAttrConfig.get() != NULL);
        packAttrConfig->EnableImpact();

        AttributeConfigPtr packDataAttrConfig = packAttrConfig->CreateAttributeConfig();
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));

        PackAttributeFormatter packFormatter;
        packFormatter.Init(packAttrConfig);
        ASSERT_EQ(8, packFormatter.mFixAttrSize);

        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, mDocStr);
        StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());
        StringView encodePackData = convertor->Decode(packedField).data;

        // 1 byte count + 8 byte fixedLen data + 9 byte int32_multi, no adaptive offsets
        ASSERT_EQ(18, encodePackData.size());
        CheckVarLenReference(packAttrConfig, "int32_multi", 8, true, 1, 0);
    }

    {
        // test two var field, last var field will store count header
        string attribute = "pack_attr:int32_single,float_single,int32_multi,str_single";
        auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
        PackAttributeConfigPtr packAttrConfig = schema->GetAttributeSchema()->GetPackAttributeConfig("pack_attr");
        ASSERT_TRUE(packAttrConfig.get() != NULL);
        packAttrConfig->EnableImpact();

        AttributeConfigPtr packDataAttrConfig = packAttrConfig->CreateAttributeConfig();
        AttributeConvertorPtr convertor(
            AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));

        PackAttributeFormatter packFormatter;
        packFormatter.Init(packAttrConfig);
        ASSERT_EQ(8, packFormatter.mFixAttrSize);

        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(schema, mDocStr);
        StringView packedField = packFormatter.Format(GetFields(&packFormatter, doc), doc->GetPool());
        StringView encodePackData = convertor->Decode(packedField).data;

        // 1 byte count + 8 byte fixedLen data + 1 byte offsetLen + 1 byte offset
        // 8 byte int32_multi + 4 byte str_single (abc with count header)
        ASSERT_EQ(23, encodePackData.size());

        CheckVarLenReference(packAttrConfig, "int32_multi", 8, false, 2, 0);
        CheckVarLenReference(packAttrConfig, "str_single", 8, true, 2, 1);
    }

    {
        // test multi var field, multi-string & last var field need var len header
        string attribute =
            "pack_attr:int32_single,float_single,int32_multi,str_multi,int8_multi,str_single,int64_multi";
        auto schema = SchemaMaker::MakeSchema(field, index, attribute, "");
        PackAttributeConfigPtr packAttrConfig = schema->GetAttributeSchema()->GetPackAttributeConfig("pack_attr");
        ASSERT_TRUE(packAttrConfig.get() != NULL);
        packAttrConfig->EnableImpact();

        CheckVarLenReference(packAttrConfig, "int32_multi", 8, false, 5, 0);
        CheckVarLenReference(packAttrConfig, "str_multi", 8, true, 5, 1);
        CheckVarLenReference(packAttrConfig, "int8_multi", 8, false, 5, 2);
        CheckVarLenReference(packAttrConfig, "str_single", 8, false, 5, 3);
        CheckVarLenReference(packAttrConfig, "int64_multi", 8, true, 5, 4);
    }
}

void PackAttributeFormatterTest::ConstructUpdateFields(const string& despStr, bool hasHashKeyInAttrField,
                                                       PackAttributeFields& updateFields)
{
    assert(updateFields.empty());
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    ASSERT_TRUE(attrSchema.get() != NULL);

    vector<vector<string>> updateValues;
    StringUtil::fromString(despStr, updateValues, ":", ";");
    for (size_t i = 0; i < updateValues.size(); ++i) {
        assert(updateValues[i].size() == 2);
        attrid_t attrId = StringUtil::numberFromString<attrid_t>(updateValues[i][0]);
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(attrId);
        assert(attrConfig);

        AttributeConvertorPtr attrConvertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
        StringView fieldValue = StringView(updateValues[i][1].c_str(), updateValues[i][1].size());
        StringView encodeValue = attrConvertor->Encode(fieldValue, mPool);
        if (hasHashKeyInAttrField) {
            updateFields.push_back(make_pair(attrId, encodeValue));
        } else {
            AttrValueMeta meta = attrConvertor->Decode(encodeValue);
            updateFields.push_back(make_pair(attrId, meta.data));
        }
    }
}

void PackAttributeFormatterTest::CheckReference(const PackAttributeFormatter& formatter,
                                                const PackAttributeConfigPtr& packConfig, const StringView& packedField,
                                                const string& expectValueStr, float diff)
{
    ASSERT_TRUE(packedField != StringView::empty_instance());

    AttributeConfigPtr packDataAttrConfig = packConfig->CreateAttributeConfig();
    AttributeConvertorPtr convertor(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));

    // use multi_value_type getData
    StringView encodePackData = convertor->Decode(packedField).data;
    size_t encodeCountLen = 0;
    MultiValueFormatter::decodeCount(encodePackData.data(), encodeCountLen);
    StringView packData = StringView(encodePackData.data() + encodeCountLen, encodePackData.size() - encodeCountLen);

    vector<string> expectValues;
    StringUtil::fromString(expectValueStr, expectValues, ",");

    const vector<AttributeConfigPtr>& attrConfigs = packConfig->GetAttributeConfigVec();
    assert(expectValues.size() == attrConfigs.size());

    for (size_t i = 0; i < attrConfigs.size(); ++i) {
        CheckAttrReference(attrConfigs[i], formatter, packData, expectValues[i], diff);
    }
}

void PackAttributeFormatterTest::CheckAttrReference(const AttributeConfigPtr& attrConfig,
                                                    const PackAttributeFormatter& formatter,
                                                    const StringView& packFieldValue, const string& expectValue,
                                                    float diff)
{
    AttributeReference* attrRef = formatter.GetAttributeReference(attrConfig->GetAttrName());
    ASSERT_TRUE(attrRef);
    ASSERT_TRUE(attrRef == formatter.GetAttributeReference(attrConfig->GetAttrId()));

    FieldType fieldType = attrConfig->GetFieldType();
    bool isMultiValue = attrConfig->IsMultiValue();
    bool isFixedLength = attrConfig->IsLengthFixed();
    if (!isMultiValue) {
        CheckSingleValueReference(fieldType, attrRef, packFieldValue.data(), expectValue, diff);
    } else if (!isFixedLength) {
        CheckMultiValueReference(fieldType, attrRef, packFieldValue.data(), expectValue);
    } else {
        CheckCountedMultiValueReference(fieldType, attrRef, packFieldValue.data(), expectValue);
    }
}

void PackAttributeFormatterTest::CheckSingleValueReference(FieldType fieldType, AttributeReference* attrRef,
                                                           const char* baseValue, const string& expectValue, float diff)
{
#define CHECK_TYPED_SINGLE_VALUE(fieldType, attrRef, baseValue, expectValue)                                           \
    case fieldType:                                                                                                    \
        CheckSingleValue<FieldTypeTraits<fieldType>::AttrItemType>(attrRef, baseValue, expectValue, diff);             \
        break;

    switch (fieldType) {
        CHECK_TYPED_SINGLE_VALUE(ft_int8, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_int16, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_int32, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_int64, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_uint8, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_uint16, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_uint32, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_uint64, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_float, attrRef, baseValue, expectValue);
        CHECK_TYPED_SINGLE_VALUE(ft_double, attrRef, baseValue, expectValue);

    case ft_string:
        CheckSingleValue<MultiChar>(attrRef, baseValue, expectValue);
        break;

    default:
        assert(false);
    }
}

void PackAttributeFormatterTest::CheckMultiValueReference(FieldType fieldType, AttributeReference* attrRef,
                                                          const char* baseValue, const string& expectValue)
{
#define CHECK_TYPED_MULTI_VALUE(fieldType, attrRef, baseValue, expectValue)                                            \
    case fieldType:                                                                                                    \
        CheckMultiValue<FieldTypeTraits<fieldType>::AttrItemType>(attrRef, baseValue, expectValue);                    \
        break;

    switch (fieldType) {
        CHECK_TYPED_MULTI_VALUE(ft_int8, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_int16, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_int32, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_int64, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_uint8, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_uint16, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_uint32, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_uint64, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_float, attrRef, baseValue, expectValue);
        CHECK_TYPED_MULTI_VALUE(ft_double, attrRef, baseValue, expectValue);

    case ft_string:
        CheckMultiValue<MultiChar>(attrRef, baseValue, expectValue);
        break;

    default:
        assert(false);
    }
}

void PackAttributeFormatterTest::CheckCountedMultiValueReference(FieldType fieldType, AttributeReference* attrRef,
                                                                 const char* baseValue, const string& expectValue)
{
#define CHECK_TYPED_COUNTED_MULTI_VALUE(fieldType, attrRef, baseValue, expectValue)                                    \
    case fieldType:                                                                                                    \
        CheckCountedMultiValue<FieldTypeTraits<fieldType>::AttrItemType>(attrRef, baseValue, expectValue);             \
        break;

    switch (fieldType) {
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_int8, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_int16, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_int32, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_int64, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_uint8, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_uint16, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_uint32, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_uint64, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_float, attrRef, baseValue, expectValue);
        CHECK_TYPED_COUNTED_MULTI_VALUE(ft_double, attrRef, baseValue, expectValue);

        // case ft_string:
        // CheckMultiValue<MultiChar>(attrRef, baseValue, expectValue);
        // break;

    default:
        assert(false);
    }
}

vector<StringView> PackAttributeFormatterTest::GetFields(PackAttributeFormatter* format, const NormalDocumentPtr& doc)
{
    vector<StringView> fields;
    const PackAttributeFormatter::FieldIdVec& fieldIds = format->GetFieldIds();
    AttributeDocumentPtr attrDoc = doc->GetAttributeDocument();

    for (auto id : fieldIds) {
        fields.push_back(attrDoc->GetField(id));
    }
    return fields;
}

void PackAttributeFormatterTest::UpdateImpactValue(const config::IndexPartitionSchemaPtr& schema)
{
    bool enableImpackValue = GET_CASE_PARAM();
    if (enableImpackValue) {
        auto attrSchema = schema->GetAttributeSchema();
        for (packattrid_t packId = 0; packId < attrSchema->GetPackAttributeCount(); packId++) {
            auto packAttrConfig = attrSchema->GetPackAttributeConfig(packId);
            if (packAttrConfig) {
                packAttrConfig->EnableImpact();
            }
        }
    }
}

}} // namespace indexlib::common

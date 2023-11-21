#include "indexlib/config/test/index_partition_schema_unittest.h"

#include <fstream>

#include "autil/EnvUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/test/modify_schema_maker.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

using namespace indexlib::test;
using namespace indexlib::file_system;

namespace indexlib { namespace config {

void IndexPartitionSchemaTest::CaseSetUp() {}

void IndexPartitionSchemaTest::CaseTearDown() {}

void IndexPartitionSchemaTest::TestCaseForJsonize()
{
    // read config file
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json";
    ifstream in(confFile.c_str());
    string line;
    string jsonString;
    while (getline(in, line)) {
        jsonString += line;
    }

    Any any = ParseJson(jsonString);
    string jsonString1 = ToString(any);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    ASSERT_EQ((int32_t)2, schema->GetSchemaVersionId());

    Any any2 = ToJson(*schema);
    string jsonString2 = ToString(any2);
    Any comparedAny = ParseJson(jsonString2);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("noname"));
    FromJson(*comparedSchema, comparedAny);
    ASSERT_FALSE(comparedSchema->GetAutoUpdatePreference());
    bool exception = false;
    try {
        schema->AssertEqual(*comparedSchema);
    } catch (const util::ExceptionBase& e) {
        exception = true;
    }
    ASSERT_TRUE(!exception);
}

void IndexPartitionSchemaTest::TestSupportNullField()
{
    // normal case
    IndexPartitionSchemaPtr tmpSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_null_field.json");
    string jsonString = ToJsonString(tmpSchema);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    auto checkField = [](const IndexPartitionSchemaPtr& schema, const std::string& fieldName, bool expectSupportNull,
                         const std::string& expectStrValue, const std::string& expectAttrNullValue = "") {
        FieldConfigPtr fieldConfig = schema->GetFieldConfig(fieldName);
        ASSERT_TRUE(fieldConfig);
        ASSERT_EQ(expectSupportNull, fieldConfig->IsEnableNullField());
        if (expectSupportNull) {
            ASSERT_EQ(expectStrValue, fieldConfig->GetNullFieldLiteralString());
        }
        ASSERT_EQ(expectAttrNullValue, fieldConfig->GetUserDefineAttributeNullValue());
    };

    checkField(schema, "price5", true, "default_null");
    checkField(schema, "catmap", true, DEFAULT_NULL_FIELD_STRING, "123456");
    checkField(schema, "price4", false, "");
    checkField(schema, "price4", false, "");
    // exception case
    ASSERT_ANY_THROW(
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema_with_null_field_for_equal_compress.json"));
    ASSERT_ANY_THROW(
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema_with_null_field_for_float_compress.json"));
    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema_with_invalid_null_field_value.json"));
    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema_with_null_field_for_packed_attr.json"));

    // check index
    auto checkIndex = [](const IndexPartitionSchemaPtr& schema, const std::string& indexName, bool expectSupportNull) {
        IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
        ASSERT_TRUE(indexConfig);
        ASSERT_EQ(expectSupportNull, indexConfig->SupportNull());
    };
    checkIndex(schema, "phrase", true);
    checkIndex(schema, "phrase2", true);
    checkIndex(schema, "catmap", true);
    checkIndex(schema, "title", false);

    // check attribute
    auto checkAttr = [](const IndexPartitionSchemaPtr& schema, const std::string& attrName, bool expectSupportNull) {
        AttributeSchemaPtr attributeSchema = schema->GetAttributeSchema();
        AttributeConfigPtr attributeConfig = attributeSchema->GetAttributeConfig(attrName);
        ASSERT_TRUE(attributeConfig);
        ASSERT_EQ(expectSupportNull, attributeConfig->SupportNull());
    };
    checkAttr(schema, "price2", false);
    checkAttr(schema, "price5", true);

    // check summary
    auto checkSummary = [](const IndexPartitionSchemaPtr& schema, const std::string& fieldName,
                           bool expectSupportNull) {
        SummarySchemaPtr summarySchema = schema->GetSummarySchema();
        std::shared_ptr<SummaryConfig> summaryConfig = summarySchema->GetSummaryConfig(fieldName);
        ASSERT_TRUE(summaryConfig);
        ASSERT_EQ(expectSupportNull, summaryConfig->SupportNull());
    };
    checkSummary(schema, "multi_value_int", true);
    checkSummary(schema, "title", false);
}

void IndexPartitionSchemaTest::TestCaseForAddVirtualAttributeConfig()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    vector<AttributeConfigPtr> attrConfigs;
    ASSERT_TRUE(!schema->AddVirtualAttributeConfigs(attrConfigs));
    AttributeSchemaPtr virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_TRUE(!virtualAttrSchema);

    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "0"));
    ASSERT_TRUE(schema->AddVirtualAttributeConfigs(attrConfigs));
    virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_TRUE(virtualAttrSchema);
    ASSERT_EQ((size_t)1, virtualAttrSchema->GetAttributeCount());

    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "0"));
    ASSERT_TRUE(!schema->AddVirtualAttributeConfigs(attrConfigs));
    virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_TRUE(virtualAttrSchema);
    ASSERT_EQ((size_t)1, virtualAttrSchema->GetAttributeCount());

    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr2", ft_int32, false, "0"));
    ASSERT_TRUE(schema->AddVirtualAttributeConfigs(attrConfigs));
    virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_TRUE(virtualAttrSchema);
    ASSERT_EQ((size_t)2, virtualAttrSchema->GetAttributeCount());
}

void IndexPartitionSchemaTest::TestCaseForCloneVirtualAttributes()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema, "field1:long;field2:uint32", "", "field1;field2", "");

    IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(subSchema, "field3:long;field4:uint32", "", "field3;field4", "");
    schema->SetSubIndexPartitionSchema(subSchema);

    IndexPartitionSchemaPtr other(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(other, "field1:long;field2:uint32", "", "field1;field2", "");
    IndexPartitionSchemaPtr otherSub(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(otherSub, "field3:long;field4:uint32", "", "field3;field4", "");
    other->SetSubIndexPartitionSchema(otherSub);

    schema->CloneVirtualAttributes(*other);

    ASSERT_TRUE(!schema->GetVirtualAttributeSchema());
    ASSERT_TRUE(!schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema());

    vector<AttributeConfigPtr> mainVirtualAttrs;
    mainVirtualAttrs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "0"));
    ASSERT_TRUE(other->AddVirtualAttributeConfigs(mainVirtualAttrs));

    schema->CloneVirtualAttributes(*other);
    ASSERT_TRUE(schema->GetVirtualAttributeSchema());
    ASSERT_EQ(size_t(1), schema->GetVirtualAttributeSchema()->GetAttributeCount());
    ASSERT_TRUE(!schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema());

    vector<AttributeConfigPtr> subVirtualAttrs;
    subVirtualAttrs.push_back(VirtualAttributeConfigCreator::Create("vir_attr2", ft_int32, false, "0"));
    ASSERT_TRUE(other->GetSubIndexPartitionSchema()->AddVirtualAttributeConfigs(subVirtualAttrs));

    const RegionSchemaImplPtr& regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID)->GetImpl();
    regionSchema->mVirtualAttributeSchema.reset();
    schema->CloneVirtualAttributes(*other);
    ASSERT_TRUE(schema->GetVirtualAttributeSchema());
    ASSERT_EQ(size_t(1), schema->GetVirtualAttributeSchema()->GetAttributeCount());
    ASSERT_TRUE(schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema());
    ASSERT_EQ(size_t(1), schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema()->GetAttributeCount());
}

void IndexPartitionSchemaTest::TestCaseForFieldConfig()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    fieldid_t fieldId = schema->GetFieldConfig("title")->GetFieldId();
    ASSERT_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = schema->GetFieldConfig("body")->GetFieldId();
    ASSERT_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = schema->GetFieldConfig("user_name")->GetFieldId();
    ASSERT_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = schema->GetFieldConfig("product_id")->GetFieldId();
    ASSERT_TRUE(indexSchema->IsInIndex(fieldId));
    FieldConfigPtr fieldConfig = schema->GetFieldConfig("price2");
    ASSERT_TRUE(fieldConfig->GetCompressType().HasPatchCompress());
    fieldConfig = schema->GetFieldConfig("price3");
    ASSERT_TRUE(fieldConfig->GetCompressType().HasPatchCompress());

    // no fileds
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "/schema/schema_without_fields.json"),
                 util::SchemaException);
}

void IndexPartitionSchemaTest::TestSchemaForShortListVbyteCompress()
{
    IndexPartitionSchemaPtr orginSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    Any any = ToJson(*orginSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, comparedAny);
    schema->AssertEqual(*orginSchema);

    // default enable shortlist vbyte compress for format_version_id = 1
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("phrase");
    ASSERT_EQ(1, indexConfig->GetIndexFormatVersionId());
    ASSERT_TRUE(indexConfig->IsShortListVbyteCompress());

    // default disable shortlist vbyte compress for format_version_id = 0
    indexConfig = indexSchema->GetIndexConfig("title");
    ASSERT_EQ(IndexConfig::DEFAULT_FORMAT_VERSION, indexConfig->GetIndexFormatVersionId());
    if (indexConfig->GetIndexFormatVersionId() == 0) {
        ASSERT_FALSE(indexConfig->IsShortListVbyteCompress());
    } else {
        ASSERT_TRUE(indexConfig->IsShortListVbyteCompress());
    }

    // enable shortlist vbyte compress for format_version_id = 0 munally
    indexConfig = indexSchema->GetIndexConfig("user_name");
    ASSERT_EQ(0, indexConfig->GetIndexFormatVersionId());
    ASSERT_TRUE(indexConfig->IsShortListVbyteCompress());

    // disable shortlist vbyte compress for format_version_id = 1 munally
    indexConfig = indexSchema->GetIndexConfig("product_id");
    ASSERT_EQ(1, indexConfig->GetIndexFormatVersionId());
    ASSERT_FALSE(indexConfig->IsShortListVbyteCompress());
}

void IndexPartitionSchemaTest::TestSchemaForIndexFormatVersionIdOverRange()
{
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "/index_engine_example_with_invalid_format_versionid.json"),
                 util::BadParameterException);
}

void IndexPartitionSchemaTest::TestCaseForGetIndexIdList()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    fieldid_t fieldId = schema->GetFieldConfig("title")->GetFieldId();

    ASSERT_EQ((size_t)3, indexSchema->GetIndexIdList(fieldId).size());
    ASSERT_EQ((indexid_t)0, indexSchema->GetIndexIdList(fieldId)[0]);
    ASSERT_EQ((indexid_t)1, indexSchema->GetIndexIdList(fieldId)[1]);
    ASSERT_EQ((indexid_t)4, indexSchema->GetIndexIdList(fieldId)[2]);

    fieldId = schema->GetFieldConfig("body")->GetFieldId();
    ASSERT_EQ((size_t)2, indexSchema->GetIndexIdList(fieldId).size());
    ASSERT_EQ((indexid_t)0, indexSchema->GetIndexIdList(fieldId)[0]);
    ASSERT_EQ((indexid_t)4, indexSchema->GetIndexIdList(fieldId)[1]);

    fieldId = schema->GetFieldConfig("user_name")->GetFieldId();
    ASSERT_EQ((size_t)1, indexSchema->GetIndexIdList(fieldId).size());
    ASSERT_EQ((indexid_t)2, indexSchema->GetIndexIdList(fieldId)[0]);

    ASSERT_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = schema->GetFieldConfig("product_id")->GetFieldId();
    ASSERT_EQ((size_t)1, indexSchema->GetIndexIdList(fieldId).size());
    ASSERT_EQ((indexid_t)3, indexSchema->GetIndexIdList(fieldId)[0]);
}

void IndexPartitionSchemaTest::TestCaseForDictSchema()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    DictionarySchemaPtr dictShema = schema->GetDictSchema();
    ASSERT_TRUE(dictShema != NULL);

    std::shared_ptr<DictionaryConfig> top10DictConfig = dictShema->GetDictionaryConfig("top10");
    ASSERT_TRUE(top10DictConfig != NULL);
    std::shared_ptr<DictionaryConfig> top100DictConfig = dictShema->GetDictionaryConfig("top100");
    ASSERT_TRUE(top100DictConfig != NULL);

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_TRUE(indexSchema != NULL);
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("phrase");
    ASSERT_TRUE(indexConfig != NULL);
    std::shared_ptr<DictionaryConfig> phraseDictConfig = indexConfig->GetDictConfig();
    ASSERT_TRUE(phraseDictConfig->CheckEqual(*top10DictConfig).IsOK());

    indexlib::index::HighFrequencyTermPostingType postingType = indexConfig->GetHighFrequencyTermPostingType();
    ASSERT_EQ(indexlib::index::hp_both, postingType);
}

void IndexPartitionSchemaTest::TestCaseForCheck()
{
    // read config file
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json";
    string jsonString;
    FslibWrapper::AtomicLoadE(confFile, jsonString);

    Any any = ParseJson(jsonString);

    // same field with 2 single field index
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
        IndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
            "user_name2", it_string, "user_name", "", false, schema->GetFieldSchema());
        schema->AddIndexConfig(indexConfig);
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // same field with 2 single field index
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        IndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
            "product_id2", it_primarykey64, "product_id", "", false, schema->GetFieldSchema());
        ASSERT_THROW(schema->AddIndexConfig(indexConfig), util::SchemaException);
    }

    // wrong field order in pack
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        PackageIndexConfigPtr packConfig =
            dynamic_pointer_cast<PackageIndexConfig>(schema->GetIndexSchema()->GetIndexConfig("phrase"));
        schema->Check();
        packConfig->AddFieldConfig("taobao_body2");
        schema->Check();
        packConfig->AddFieldConfig("taobao_body1");
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // wrong field order in expack
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        PackageIndexConfigPtr packConfig =
            dynamic_pointer_cast<PackageIndexConfig>(schema->GetIndexSchema()->GetIndexConfig("phrase2"));
        schema->Check();
        packConfig->AddFieldConfig("taobao_body2");
        schema->Check();
        packConfig->AddFieldConfig("taobao_body1");
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // upsupport attribute
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
        schema->AddFieldConfig("TestField1", ft_text);
        ASSERT_THROW(schema->AddAttributeConfig("TestField1"), util::SchemaException);

        schema->AddFieldConfig("TestField2", ft_enum);
        ASSERT_THROW(schema->AddAttributeConfig("TestField2"), util::SchemaException);

        schema->AddFieldConfig("TestField3", ft_time);
        ASSERT_NO_THROW(schema->AddAttributeConfig("TestField3"));

        schema->AddFieldConfig("TestField6", ft_online);
        ASSERT_THROW(schema->AddAttributeConfig("TestField6"), util::SchemaException);

        schema->AddFieldConfig("TestField7", ft_property);
        ASSERT_THROW(schema->AddAttributeConfig("TestField7"), util::SchemaException);

        schema->AddFieldConfig("TestField8", ft_hash_64, true);
        ASSERT_THROW(schema->AddAttributeConfig("TestField8"), util::SchemaException);

        schema->AddFieldConfig("TestField9", ft_hash_128, true);
        ASSERT_THROW(schema->AddAttributeConfig("TestField9"), util::SchemaException);
    }

    // check option flag
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("phrase");

        indexConfig->SetOptionFlag(of_position_list);
        schema->Check();

        indexConfig->SetOptionFlag(of_position_payload);
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // check truncate
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_NO_THROW(FromJson(*schema, any));
        TruncateProfileConfigPtr config =
            schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_product_id");
        SortParams& sortParams = const_cast<SortParams&>(config->GetTruncateSortParams());

        sortParams[0].SetSortField("price");
        ASSERT_THROW(schema->Check(), util::SchemaException);
        sortParams[0].SetSortField("DOC_PAYLOAD");
        schema->Check();
        sortParams[0].SetSortField("price1");
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }
    // check schema dict hash not support pack/expack/customized
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
        FieldConfigPtr fieldConfig = schema->GetFieldConfig("title");
        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "default"}}));
        ASSERT_NO_THROW(schema->Check());

        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "LayerHash"}}));
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }
    // check sub schema duplicated fields
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                                              "text1:text;text2:text", // Field schema
                                              "pk:PRIMARYKEY64:text2", // index schema
                                              "",                      // attribute schema
                                              "");                     // Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "text1:text;text3:text", "subPk:PRIMARYKEY64:text3", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // check sub schema duplicated index
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                                              "text1:text;text2:text",                   // Field schema
                                              "pk:PRIMARYKEY64:text2;pack1:pack:text1;", // index schema
                                              "",                                        // attribute schema
                                              "");                                       // Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "text3:text;text4:text;",
                                              "pack1:pack:text3;subPk:PRIMARYKEY64:text4", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // check main schema has no primary key with sub schema
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                                              "text1:text;",      // Field schema
                                              "pack:pack:text1;", // index schema
                                              "",                 // attribute schema
                                              "");                // Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "text2:text;text3:text",
                                              "pack1:pack:text2;pk:PRIMARYKEY64:text3;", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // check sub schema has no primary key
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                                              "text1:text;text3:text",                  // Field schema
                                              "pk:PRIMARYKEY64:text3;pack:pack:text1;", // index schema
                                              "",                                       // attribute schema
                                              "");                                      // Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "text2:text;", "pack1:pack:text2;", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }

    // check sub schema check fail
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                                              "text1:text;text2:text", // Field schema
                                              "pk:PRIMARYKEY64:text2", // index schema
                                              "",                      // attribute schema
                                              "");                     // Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "text3:text;text4:text",
                                              "subPk:PRIMARYKEY64:text3;textIndex1:text:text4;textIndex2:text:text4",
                                              "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }
}

void IndexPartitionSchemaTest::TestCaseFor32FieldCountInPack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 33;
    for (uint32_t i = 0; i < fieldCount; ++i) {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;
    try {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < 32; ++i) {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }
        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
            "TestForPack32", it_pack, fieldNames, boosts, "", false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    } catch (const util::SchemaException& e) {
        catched = true;
    }
    ASSERT_TRUE(!catched);
}

void IndexPartitionSchemaTest::TestCaseFor33FieldCountInPack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 33;
    for (uint32_t i = 0; i < fieldCount; ++i) {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;

    try {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < fieldCount; ++i) {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }

        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
            "TestForPack33", it_pack, fieldNames, boosts, "", false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    } catch (const util::SchemaException& e) {
        catched = true;
    }
    ASSERT_TRUE(catched);
}

void IndexPartitionSchemaTest::TestCaseFor9FieldCountInExpack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 9;
    for (uint32_t i = 0; i < fieldCount; ++i) {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;
    try {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < fieldCount; ++i) {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }
        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
            "TestForExpack9", it_expack, fieldNames, boosts, "", false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    } catch (const util::SchemaException& e) {
        catched = true;
    }
    ASSERT_TRUE(catched);
}

void IndexPartitionSchemaTest::TestCaseFor8FieldCountInExpack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 9;
    for (uint32_t i = 0; i < fieldCount; ++i) {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;
    try {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < 8; ++i) {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }

        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
            "TestForExpack8", it_expack, fieldNames, boosts, "", false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    } catch (const util::SchemaException& e) {
        catched = true;
    }
    ASSERT_TRUE(!catched);
}

void IndexPartitionSchemaTest::TestCaseForJsonizeWithSubSchema()
{
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example_with_sub_schema.json";
    string jsonString;
    FslibWrapper::AtomicLoadE(confFile, jsonString);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    schema->Check();

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    ASSERT_TRUE(subSchema);
    ASSERT_TRUE(subSchema->GetFieldConfig("sub_title"));

    Any any2 = ToJson(*schema);
    string jsonString2 = ToString(any2);
    Any comparedAny = ParseJson(jsonString2);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("noname"));
    FromJson(*comparedSchema, comparedAny);
    ASSERT_NO_THROW(schema->AssertEqual(*comparedSchema));
}

void IndexPartitionSchemaTest::TestCaseForJsonizeWithTruncate()
{
    // read config file
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example_with_truncate.json";
    string jsonString;
    FslibWrapper::AtomicLoadE(confFile, jsonString);
    Any any = ParseJson(jsonString);
    string jsonString1 = ToString(any);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    schema->Check();

    Any any2 = ToJson(*schema);
    string jsonString2 = ToString(any2);
    Any comparedAny = ParseJson(jsonString2);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("noname"));
    FromJson(*comparedSchema, comparedAny);
    ASSERT_NO_THROW(schema->AssertEqual(*comparedSchema));
}

void IndexPartitionSchemaTest::TestCaseForTruncate()
{
    IndexPartitionSchemaPtr schema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example_with_truncate.json");
    ASSERT_NO_THROW(schema->Check());
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_TRUE(indexSchema->GetIndexConfig("phrase")->HasTruncate());
    ASSERT_TRUE(indexSchema->GetIndexConfig("phrase")->HasTruncateProfile(
        schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_product_id").get()));
    ASSERT_TRUE(indexSchema->GetIndexConfig("phrase")->HasTruncateProfile(
        schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_user_name").get()));
    ASSERT_TRUE(indexSchema->GetIndexConfig("phrase_desc_product_id"));
    ASSERT_TRUE(indexSchema->GetIndexConfig("phrase_desc_product_id")->IsVirtual());
    ASSERT_TRUE(indexSchema->GetIndexConfig("phrase_desc_user_name"));
    ASSERT_TRUE(indexSchema->GetIndexConfig("phrase_desc_user_name")->IsVirtual());

    ASSERT_TRUE(indexSchema->GetIndexConfig("title")->HasTruncate());
    ASSERT_TRUE(indexSchema->GetIndexConfig("title")->HasTruncateProfile(
        schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_product_id").get()));
    ASSERT_TRUE(!indexSchema->GetIndexConfig("title")->HasTruncateProfile(
        schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_user_name").get()));
    ASSERT_TRUE(indexSchema->GetIndexConfig("title_desc_product_id"));
    ASSERT_TRUE(!indexSchema->GetIndexConfig("title_desc_user_name"));
    ASSERT_TRUE(indexSchema->GetIndexConfig("title_desc_product_id")->IsVirtual());

    ASSERT_TRUE(indexSchema->GetIndexConfig("user_name")->HasTruncate());
    ASSERT_TRUE(indexSchema->GetIndexConfig("user_name")
                    ->HasTruncateProfile(
                        schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_product_id").get()));
    ASSERT_TRUE(
        indexSchema->GetIndexConfig("user_name")
            ->HasTruncateProfile(schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_user_name").get()));
    ASSERT_TRUE(indexSchema->GetIndexConfig("user_name_desc_product_id"));
    ASSERT_TRUE(indexSchema->GetIndexConfig("user_name_desc_product_id")->IsVirtual());
    ASSERT_TRUE(indexSchema->GetIndexConfig("user_name_desc_user_name"));
    ASSERT_TRUE(indexSchema->GetIndexConfig("user_name_desc_user_name")->IsVirtual());

    ASSERT_TRUE(!indexSchema->GetIndexConfig("product_id")->HasTruncate());
    ASSERT_TRUE(!indexSchema->GetIndexConfig("phrase2")->HasTruncate());
    ASSERT_TRUE(!indexSchema->GetIndexConfig("categoryp")->HasTruncate());
    ASSERT_TRUE(!indexSchema->GetIndexConfig("catmap")->HasTruncate());

    // truncate not support single compressed float
    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "truncate_with_compressed_float.json"));
    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "truncate_with_fp8.json"));
}

void IndexPartitionSchemaTest::TestCaseForTruncateCheck()
{
    {
        IndexPartitionSchemaPtr schema =
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example_with_truncate.json");

        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        const IndexConfigPtr& indexConfig = indexSchema->GetIndexConfig("title");
        indexConfig->SetUseTruncateProfiles({"bad_profile"});
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }
    {
        IndexPartitionSchemaPtr schema =
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example_with_truncate.json");

        TruncateProfileConfigPtr config =
            schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_product_id");
        SortParams& sortParams = const_cast<SortParams&>(config->GetTruncateSortParams());

        sortParams[0].SetSortField("non-exist-field");
        ASSERT_THROW(schema->Check(), util::SchemaException);

        sortParams[0].SetSortField("non-corresponding-attribute-field");
        ASSERT_THROW(schema->Check(), util::SchemaException);
    }
    {
        // truncate sort field name appear in pack attribute
        ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/" +
                                "pack_attribute_schema_with_truncate_error.json"),
                     util::SchemaException);
    }
}

IndexPartitionSchemaPtr IndexPartitionSchemaTest::ReadSchema(const string& fileName)
{
    // read config file
    ifstream in(fileName.c_str());
    string line;
    string jsonString;
    while (getline(in, line)) {
        jsonString += line;
    }

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    Any any = ParseJson(jsonString);
    FromJson(*schema, any);
    return schema;
}

void IndexPartitionSchemaTest::TestAssertCompatible()
{
    // check sub schema compatible

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
                                          "title:text;field1:long", // Field schema
                                          "title:text:title",       // index schema
                                          "field1",                 // attribute schema
                                          "");                      // Summary schema
    {
        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        ASSERT_NO_THROW(schema->AssertCompatible(*otherSchema));
    }
    {
        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        AttributeConfigPtr attrConfig = otherSchema->GetAttributeSchema()->GetAttributeConfig("field1");
        std::shared_ptr<FileCompressConfig> fileCompressConfig(new FileCompressConfig());
        attrConfig->SetFileCompressConfig(fileCompressConfig);
        const IndexPartitionSchemaImplPtr& tempImpl = schema->GetImpl();
        const IndexPartitionSchemaImplPtr& otherImpl = otherSchema->GetImpl();
        ASSERT_THROW(tempImpl->DoAssertCompatible(*(otherImpl.get())), util::BadParameterException);
    }
    {
        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "sub_title:text;", "sub_title:text:sub_title", "", "");
        otherSchema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_NO_THROW(schema->AssertCompatible(*otherSchema));
    }
    {
        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        IndexPartitionSchemaPtr tempSchema(schema->Clone());

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "sub_title:text;", "sub_title:text:sub_title", "", "");
        tempSchema->SetSubIndexPartitionSchema(subSchema);

        const IndexPartitionSchemaImplPtr& tempImpl = tempSchema->GetImpl();
        const IndexPartitionSchemaImplPtr& otherImpl = otherSchema->GetImpl();
        ASSERT_THROW(tempImpl->DoAssertCompatible(*(otherImpl.get())), util::AssertCompatibleException);
    }
    {
        IndexPartitionSchemaPtr tempSchema(schema->Clone());
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "sub_title:text;", "sub_title:text:sub_title", "", "");
        tempSchema->SetSubIndexPartitionSchema(subSchema);

        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        IndexPartitionSchemaPtr otherSubSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(otherSubSchema, "sub_title:text;", "sub_title2:text:sub_title", "", "");
        subSchema->SetSubIndexPartitionSchema(otherSubSchema);

        const IndexPartitionSchemaImplPtr& tempImpl = tempSchema->GetImpl();
        const IndexPartitionSchemaImplPtr& otherImpl = otherSchema->GetImpl();
        ASSERT_THROW(tempImpl->DoAssertCompatible(*(otherImpl.get())), util::AssertCompatibleException);
    }
}

void IndexPartitionSchemaTest::TestCaseForIsUsefulField()
{
    IndexPartitionSchemaPtr tempSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(tempSchema, "title:text;useless_field:string;", "title:text:title", "", "");
    {
        ASSERT_TRUE(tempSchema->IsUsefulField("title"));
        ASSERT_FALSE(tempSchema->IsUsefulField("useless_field"));
        ASSERT_FALSE(tempSchema->IsUsefulField("no_such_field"));
    }
    {
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "sub_title:text;useless_sub_field:string;",
                                              "sub_title:text:sub_title", "", "");
        tempSchema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_TRUE(tempSchema->IsUsefulField("title"));
        ASSERT_TRUE(tempSchema->IsUsefulField("sub_title"));
        ASSERT_FALSE(tempSchema->IsUsefulField("useless_sub_field"));
        ASSERT_FALSE(tempSchema->IsUsefulField("no_such_field"));
    }
}

void IndexPartitionSchemaTest::TestCaseForVirtualField()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    schema->Check();
    schema->AddFieldConfig("virtual_field", ft_int32, false, true);
    schema->AddAttributeConfig("virtual_field");

    string schemaString = ToJsonString(*schema);
    IndexPartitionSchemaPtr loadSchema(new IndexPartitionSchema);
    FromJsonString(*loadSchema, schemaString);

    FieldConfigPtr loadFieldConfig = loadSchema->GetFieldConfig("virtual_field");
    ASSERT_TRUE(loadFieldConfig->IsVirtual());
    AttributeConfigPtr loadAttributeConfig = loadSchema->GetAttributeSchema()->GetAttributeConfig("virtual_field");
    ASSERT_TRUE(loadAttributeConfig);
}

void IndexPartitionSchemaTest::TestCaseForVirtualAttribute()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    ASSERT_TRUE(schema->GetAttributeSchema()->GetAttributeConfig("price2"));
    uint32_t fieldCount = schema->GetFieldCount();

    FieldConfigPtr fieldConfig(new FieldConfig("virtual_attribute", ft_int32, false));
    AttributeConfigPtr attrConfig(new AttributeConfig(AttributeConfig::ct_virtual));
    attrConfig->Init(fieldConfig, common::AttributeValueInitializerCreatorPtr());

    // add virtual attribute
    ASSERT_NO_THROW(schema->AddVirtualAttributeConfig(attrConfig));

    // name already used in attribute
    fieldConfig->SetFieldName("price2");
    ASSERT_THROW(schema->AddVirtualAttributeConfig(attrConfig), util::SchemaException);

    // name already used in virtual attribute
    fieldConfig->SetFieldName("virtual_attribute");
    ASSERT_THROW(schema->AddVirtualAttributeConfig(attrConfig), util::SchemaException);

    // add another virtual attribute
    fieldConfig->SetFieldName("virtual_attribute_1");
    ASSERT_NO_THROW(schema->AddVirtualAttributeConfig(attrConfig));

    // check schema
    const AttributeSchemaPtr& virtualAttrSchema = schema->GetVirtualAttributeSchema();
    ASSERT_TRUE(virtualAttrSchema);
    CheckVirtualAttribute(virtualAttrSchema, "virtual_attribute", fieldCount);
    CheckVirtualAttribute(virtualAttrSchema, "virtual_attribute_1", fieldCount + 1);
}

void IndexPartitionSchemaTest::TestCaseForPackAttribute()
{
    IndexPartitionSchemaPtr onDiskSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example_with_pack_attribute.json");
    ASSERT_EQ("auction", onDiskSchema->GetSchemaName());
    onDiskSchema->SetSchemaName("noname");

    // test jsonize
    string str = ToJsonString(onDiskSchema);
    Any jsonAny = ParseJson(str);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(onDiskSchema->GetSchemaName()));
    FromJson(*schema, jsonAny);

    ASSERT_NO_THROW(schema->AssertEqual(*onDiskSchema));

    // test get attribute configs
    AttributeSchemaPtr attributeSchema = schema->GetAttributeSchema();
    ASSERT_TRUE(attributeSchema);
    ASSERT_EQ(size_t(8), attributeSchema->GetAttributeCount());
    ASSERT_EQ(size_t(2), attributeSchema->GetPackAttributeCount());

    ASSERT_FALSE(attributeSchema->GetPackAttributeConfig("user_id"));
    ASSERT_FALSE(attributeSchema->GetPackAttributeConfig("user_name"));
    ASSERT_TRUE(attributeSchema->GetPackAttributeConfig("product_info"));
    ASSERT_TRUE(attributeSchema->GetPackAttributeConfig("product_price"));

    // test get pack attribute from normal attribute
    const AttributeConfigPtr& attrConfig = attributeSchema->GetAttributeConfig("product_id");

    attrid_t valid_id = attrConfig->GetAttrId();
    attrid_t invalid_id = 10;

    ASSERT_EQ(attributeSchema->GetPackAttributeConfig("product_info")->GetPackAttrId(),
              attributeSchema->GetPackIdByAttributeId(valid_id));
    ASSERT_EQ(INVALID_PACK_ATTRID, attributeSchema->GetPackIdByAttributeId(invalid_id));

    // test pack attribute config

    PackAttributeConfigPtr packAttrConfig = attributeSchema->GetPackAttributeConfig("product_info");
    ASSERT_FALSE(packAttrConfig->HasEnableImpact());
    ASSERT_TRUE(packAttrConfig->HasEnablePlainFormat());
    ASSERT_EQ(string("product_info"), packAttrConfig->GetPackName());
    ASSERT_EQ(attrid_t(0), packAttrConfig->GetPackAttrId());
    vector<string> subAttrNames;
    packAttrConfig->GetSubAttributeNames(subAttrNames);
    ASSERT_EQ(size_t(2), subAttrNames.size());
    ASSERT_EQ(string("product_id"), subAttrNames[0]);
    ASSERT_EQ(string("category"), subAttrNames[1]);
    CompressTypeOption compType = packAttrConfig->GetCompressType();
    ASSERT_TRUE(compType.HasEquivalentCompress());
    ASSERT_FALSE(compType.HasUniqEncodeCompress());

    packAttrConfig = attributeSchema->GetPackAttributeConfig("product_price");
    ASSERT_TRUE(packAttrConfig->HasEnableImpact());
    ASSERT_FALSE(packAttrConfig->HasEnablePlainFormat());
    ASSERT_EQ(attrid_t(1), packAttrConfig->GetPackAttrId());
    compType = packAttrConfig->GetCompressType();
    ASSERT_EQ(string("product_price"), packAttrConfig->GetPackName());
    packAttrConfig->GetSubAttributeNames(subAttrNames);
    ASSERT_EQ(size_t(4), subAttrNames.size());
    ASSERT_EQ(string("price2"), subAttrNames[0]);
    ASSERT_EQ(string("price3"), subAttrNames[1]);
    ASSERT_EQ(string("price4"), subAttrNames[2]);
    ASSERT_EQ(string("price5"), subAttrNames[3]);
    ASSERT_FALSE(compType.HasCompressOption());
}

void IndexPartitionSchemaTest::TestCaseForPackAttributeWithError()
{
    ASSERT_THROW(
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/" + "pack_attribute_schema_with_error1.json"),
        util::BadParameterException);
    ASSERT_THROW(
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/" + "pack_attribute_schema_with_error2.json"),
        util::SchemaException);
    ASSERT_THROW(
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/" + "pack_attribute_schema_with_error3.json"),
        util::SchemaException);
    ASSERT_THROW(
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/" + "pack_attribute_schema_with_error4.json"),
        util::SchemaException);
    ASSERT_THROW(
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "pack_attribute/" + "main_sub_schema_with_duplicate_pack_attr.json"),
        util::SchemaException);
}

void IndexPartitionSchemaTest::TestCaseForKVTTL()
{
    // "time_to_live" : 20
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/kv_schema_default.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(20L, schema->GetDefaultTTL());
    string jsonString = ToJsonString(*schema);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(20L, cloneSchema->GetDefaultTTL());

    // "enable_ttl": true
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/kv_schema_enable.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // "time_to_live" : 30, "enable_ttl": true
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/kv_schema_both_1.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(30L, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(30L, cloneSchema->GetDefaultTTL());

    // "time_to_live" : 20, "enable_ttl": false
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/kv_schema_both_2.json");
    ASSERT_FALSE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_FALSE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // "time_to_live" : -1, "enable_ttl": true
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/kv_schema_both_3.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // no ttl
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/kv_schema_nottl.json");
    ASSERT_FALSE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_FALSE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());
}

void IndexPartitionSchemaTest::TestCaseForNormalTTL()
{
    // "time_to_live" : 20
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/normal_schema_default.json");
    EXPECT_TRUE(schema->TTLEnabled());
    EXPECT_EQ(20L, schema->GetDefaultTTL());
    string jsonString = ToJsonString(*schema);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    EXPECT_TRUE(cloneSchema->TTLEnabled());
    EXPECT_EQ(20L, cloneSchema->GetDefaultTTL());

    // "enable_ttl": true
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/normal_schema_enable.json");
    EXPECT_TRUE(schema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    EXPECT_TRUE(cloneSchema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // "time_to_live" : 30, "enable_ttl": true
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/normal_schema_both_1.json");
    EXPECT_TRUE(schema->TTLEnabled());
    EXPECT_EQ(30L, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    EXPECT_TRUE(cloneSchema->TTLEnabled());
    EXPECT_EQ(30L, cloneSchema->GetDefaultTTL());

    // "time_to_live" : 20, "enable_ttl": false
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/normal_schema_both_2.json");
    EXPECT_FALSE(schema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    EXPECT_FALSE(cloneSchema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // "time_to_live" : -1, "enable_ttl": true
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/normal_schema_both_3.json");
    EXPECT_TRUE(schema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    EXPECT_TRUE(cloneSchema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // no ttl
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/normal_schema_nottl.json");
    EXPECT_FALSE(schema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    EXPECT_FALSE(cloneSchema->TTLEnabled());
    EXPECT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());
}

void IndexPartitionSchemaTest::TestCaseForEnableTTL()
{
    {
        // 1. normal case
        IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/schema1.json");
        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        ASSERT_TRUE(attrSchema != NULL);
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
        ASSERT_TRUE(attrConfig != NULL);
        FieldConfigPtr fieldConfig = schema->GetFieldConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
        ASSERT_EQ(attrConfig->GetFieldConfig(), fieldConfig);
        ASSERT_EQ(ft_uint32, fieldConfig->GetFieldType());
        ASSERT_TRUE(fieldConfig->IsBuiltInField());
        ASSERT_FALSE(attrConfig->IsMultiValue());
        ASSERT_TRUE(attrConfig->IsBuiltInAttribute());
        ASSERT_EQ(fieldid_t(3), fieldConfig->GetFieldId());

        // jsonize
        string str = ToJsonString(schema);
        Any comparedAny = ParseJson(str);
        IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("noname"));
        FromJson(*comparedSchema, comparedAny);

        schema->AssertEqual(*comparedSchema);
    }
    {
        // test ttl_field_name
        IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/schema3.json");
        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        ASSERT_TRUE(attrSchema != NULL);
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
        ASSERT_TRUE(attrConfig == NULL);
        attrConfig = attrSchema->GetAttributeConfig("ttl_field");
        ASSERT_TRUE(attrConfig != NULL);
        FieldConfigPtr fieldConfig = schema->GetFieldConfig("ttl_field");
        ASSERT_EQ(attrConfig->GetFieldConfig(), fieldConfig);
        ASSERT_EQ(ft_uint32, fieldConfig->GetFieldType());
        ASSERT_FALSE(attrConfig->IsMultiValue());
        ASSERT_EQ(fieldid_t(3), fieldConfig->GetFieldId());

        // jsonize
        string str = ToJsonString(schema);
        Any comparedAny = ParseJson(str);
        IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("noname"));
        FromJson(*comparedSchema, comparedAny);

        schema->AssertEqual(*comparedSchema);
    }
    {
        IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/schema2.json");
        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        ASSERT_TRUE(attrSchema != NULL);
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
        ASSERT_TRUE(attrConfig != NULL);
        ASSERT_EQ(fieldid_t(1), attrConfig->GetFieldConfig()->GetFieldId());
    }
    {
        bool hasException = false;
        try {
            IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "ttl_test/invalid_schema.json");
        } catch (const util::SchemaException& e) {
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }
}

void IndexPartitionSchemaTest::CheckVirtualAttribute(const AttributeSchemaPtr& virtualAttrSchema,
                                                     const string& fieldName, fieldid_t fieldId)
{
    assert(virtualAttrSchema);
    AttributeConfigPtr virtualAttrConfig = virtualAttrSchema->GetAttributeConfig(fieldName);
    ASSERT_TRUE(virtualAttrConfig);
    ASSERT_EQ(fieldId, virtualAttrConfig->GetFieldId());
}

void IndexPartitionSchemaTest::TestCaseForKVIndexException()
{
    // unsupported compress type
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index_exception.json"), util::BadParameterException);
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index_exception1.json"), util::SchemaException);

    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index_exception2.json"), util::SchemaException);
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index_with_no_attribute.json"), util::SchemaException);
    // lack of fields
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_schema/kv_without_fields.json"), util::SchemaException);
    // fields in region schema
    ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_schema/kv_with_fields_in_region_schema.json");

    // field with enable null
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_with_support_null_field.json"),
                 util::BadParameterException);
}

void IndexPartitionSchemaTest::TestCaseForKVIndex()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index.json");

    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->Check();
    schema->AssertEqual(*loadSchema);
    ASSERT_EQ("kv_table", schema->GetSchemaName());
    ASSERT_EQ(tt_kv, schema->GetTableType());
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KVIndexConfigPtr kvIndexConfig = std::dynamic_pointer_cast<KVIndexConfig>(indexConfig);
    ASSERT_TRUE(kvIndexConfig);

    PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(attrConfig);
    vector<string> attrNames;
    attrConfig->GetSubAttributeNames(attrNames);
    ASSERT_THAT(attrNames, testing::ElementsAre(string("nid"), string("pidvid"), string("timestamp")));

    const KVIndexPreference& indexPreference = kvIndexConfig->GetIndexPreference();
    ASSERT_EQ(ipt_perf, indexPreference.GetType());
    const KVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
    ASSERT_FALSE(valueParam.IsEncode());
    ASSERT_EQ(string(""), valueParam.GetFileCompressType());
}

void IndexPartitionSchemaTest::TestCaseForKKVIndexException()
{
    // pkey set limit_count
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_exception1.json"), util::SchemaException);

    // skey trunc_sort_param set no_exist field
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_exception2.json"), util::SchemaException);

    // skey trunc_sort_param use multi value field
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_exception3.json"), util::SchemaException);

    // unsupported compress type
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_exception4.json"), util::BadParameterException);

    // key use multi value field
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_exception5.json"), util::SchemaException);

    // skey use multi value field
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_exception6.json"), util::SchemaException);

    // unsupported null field
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_with_null_field.json"),
                 util::BadParameterException);
}

void IndexPartitionSchemaTest::TestCaseForKKVIndex()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index.json");

    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->Check();
    schema->AssertEqual(*loadSchema);
    ASSERT_EQ("kkv_table", schema->GetSchemaName());
    ASSERT_EQ(tt_kkv, schema->GetTableType());
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    ASSERT_EQ(it_kkv, indexSchema->GetPrimaryKeyIndexType());
    SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    KKVIndexConfigPtr kkvIndexConfig = std::dynamic_pointer_cast<KKVIndexConfig>(indexConfig);
    ASSERT_TRUE(kkvIndexConfig);

    ASSERT_TRUE(kkvIndexConfig->NeedSuffixKeyTruncate());
    ASSERT_EQ((uint32_t)5000, kkvIndexConfig->GetSuffixKeyTruncateLimits());

    const SortParams& truncSortParam = kkvIndexConfig->GetSuffixKeyTruncateParams();
    ASSERT_EQ((size_t)1, truncSortParam.size());
    ASSERT_EQ("$TIME_STAMP", truncSortParam[0].GetSortField());
    ASSERT_TRUE(kkvIndexConfig->OptimizedStoreSKey());

    PackAttributeConfigPtr valueConfig = kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(valueConfig);
    vector<string> attrNames;
    valueConfig->GetSubAttributeNames(attrNames);
    ASSERT_THAT(attrNames, testing::ElementsAre(string("nick"), string("pidvid"), string("timestamp")));

    const KKVIndexPreference& indexPreference = kkvIndexConfig->GetIndexPreference();
    ASSERT_EQ(ipt_perf, indexPreference.GetType());
    const KKVIndexPreference::SuffixKeyParam& skeyParam = indexPreference.GetSkeyParam();
    const KKVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
    ASSERT_FALSE(skeyParam.IsEncode());
    ASSERT_TRUE(valueParam.IsEncode());
    ASSERT_EQ(string(""), skeyParam.GetFileCompressType());
    ASSERT_EQ(string("lz4"), valueParam.GetFileCompressType());
}

void IndexPartitionSchemaTest::TestCaseForUserDefinedParam()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    auto& jsonMap = schema->GetUserDefinedParam();
    EXPECT_TRUE(jsonMap.end() != jsonMap.find("array"));
    EXPECT_TRUE(jsonMap.end() != jsonMap.find("file_name"));
    EXPECT_TRUE(jsonMap.end() != jsonMap.find("field_separator"));
    EXPECT_TRUE(jsonMap.end() == jsonMap.find("not_exist"));
    jsonMap["new_field"] = Any(string("new_value"));
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema(""));
    Any newAny = ParseJson(ToString(ToJson(*schema)));
    FromJson(*newSchema, newAny);
    auto jsonMap2 = newSchema->GetUserDefinedParam();
    EXPECT_TRUE(jsonMap2.end() != jsonMap2.find("array"));
    EXPECT_TRUE(jsonMap2.end() != jsonMap2.find("new_field"));
    EXPECT_TRUE(jsonMap2.end() == jsonMap2.find("not_exist"));

    string value;
    EXPECT_FALSE(newSchema->GetValueFromUserDefinedParam("array", value));
    EXPECT_FALSE(newSchema->GetValueFromUserDefinedParam("not_exist", value));
    EXPECT_TRUE(newSchema->GetValueFromUserDefinedParam("file_name", value));
    EXPECT_EQ("cc", value);
    EXPECT_TRUE(newSchema->GetValueFromUserDefinedParam("field_separator", value));
    EXPECT_EQ("bb", value);
    EXPECT_TRUE(newSchema->GetValueFromUserDefinedParam("new_field", value));
    EXPECT_EQ("new_value", value);
}

void IndexPartitionSchemaTest::TestCaseForMultiRegionKVSchemaWithRegionFieldSchema()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index_with_region_field_schema.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->Check();
    schema->AssertEqual(*loadSchema);
    ASSERT_EQ("kv_table", schema->GetSchemaName());
    ASSERT_EQ(tt_kv, schema->GetTableType());
    ASSERT_EQ((size_t)2, schema->GetRegionCount());

    {
        // region1
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("region1");
        ASSERT_TRUE(regionSchema);

        ASSERT_EQ(ft_int64, regionSchema->GetFieldSchema()->GetFieldConfig("nid")->GetFieldType());
        ASSERT_TRUE(regionSchema->GetFieldSchema()->GetFieldConfig("pidvid")->IsMultiValue());

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = std::dynamic_pointer_cast<KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, testing::ElementsAre(string("nid"), string("pidvid"), string("timestamp")));

        const KVIndexPreference& indexPreference = kvIndexConfig->GetIndexPreference();
        ASSERT_EQ(ipt_perf, indexPreference.GetType());
        const KVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
        ASSERT_FALSE(valueParam.IsEncode());
        ASSERT_EQ(string(""), valueParam.GetFileCompressType());
        ASSERT_EQ((size_t)2, kvIndexConfig->GetRegionCount());
        ASSERT_EQ((regionid_t)0, kvIndexConfig->GetRegionId());
    }

    {
        // region2
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("region2");
        ASSERT_TRUE(regionSchema);

        ASSERT_EQ(ft_int32, regionSchema->GetFieldSchema()->GetFieldConfig("nid")->GetFieldType());
        ASSERT_FALSE(regionSchema->GetFieldSchema()->GetFieldConfig("pidvid")->IsMultiValue());

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = std::dynamic_pointer_cast<KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, testing::ElementsAre(string("nid"), string("pidvid")));

        const KVIndexPreference& indexPreference = kvIndexConfig->GetIndexPreference();
        ASSERT_EQ(ipt_perf, indexPreference.GetType());
        const KVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
        ASSERT_FALSE(valueParam.IsEncode());
        ASSERT_EQ(string(""), valueParam.GetFileCompressType());
        ASSERT_EQ((size_t)2, kvIndexConfig->GetRegionCount());
        ASSERT_EQ((regionid_t)1, kvIndexConfig->GetRegionId());
    }
    ASSERT_TRUE(!schema->GetGlobalRegionIndexPreference().empty());
}

void IndexPartitionSchemaTest::TestCaseForModifyOperation()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_modify_operations_2.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->Check();
    schema->AssertEqual(*loadSchema);

    CheckFieldSchema(schema->GetFieldSchema(), "string1:normal;string2:normal;price:normal;"
                                               "nid:delete;new_nid:normal;nid:normal");

    CheckIndexSchema(schema->GetIndexSchema(), "index2:delete;pk:normal;nid:delete;new_nid:normal;nid:normal");

    CheckAttributeSchema(schema->GetAttributeSchema(), "string1:normal;string2:delete;price:delete;"
                                                       "nid:delete;new_nid:normal;nid:normal");

    ASSERT_TRUE(schema->HasModifyOperations());
    ASSERT_EQ(3, schema->GetModifyOperationCount());
    size_t i = 1;
    for (; i <= schema->GetModifyOperationCount(); i++) {
        auto op = schema->GetSchemaModifyOperation(i);
        ASSERT_TRUE(op);
        ASSERT_EQ((schema_opid_t)i, op->GetOpId());
        ASSERT_TRUE(!op->GetParams().empty());
    }
    ASSERT_FALSE(schema->GetSchemaModifyOperation(i));
}

void IndexPartitionSchemaTest::TestDeleteMultiTimes()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_modify_operations_2.json");
    {
        // delete index & attribute, add them again
        IndexConfigIteratorPtr indexConfigs = loadSchema->GetIndexSchema()->CreateIterator(true, is_deleted);
        vector<string> deletedIndexName;
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++) {
            deletedIndexName.push_back((*iter)->GetIndexName());
        }
        ASSERT_THAT(deletedIndexName, testing::ElementsAre("index2", "nid"));

        AttributeConfigIteratorPtr attrConfigs = loadSchema->GetAttributeSchema()->CreateIterator(is_deleted);
        vector<string> deletedAttrName;
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
            deletedAttrName.push_back((*iter)->GetAttrName());
        }
        ASSERT_THAT(deletedAttrName, testing::ElementsAre("string2", "price", "nid"));
    }
    {
        // make add attribute not ready
        loadSchema->MarkOngoingModifyOperation(3);
        AttributeConfigIteratorPtr attrConfigs = loadSchema->GetAttributeSchema()->CreateIterator(is_disable);
        vector<string> disableAttrName;
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
            disableAttrName.push_back((*iter)->GetAttrName());
        }
        ASSERT_THAT(disableAttrName, testing::ElementsAre("nid"));
        attrConfigs = loadSchema->GetAttributeSchema()->CreateIterator(is_deleted);
        vector<string> deletedAttrName;
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
            deletedAttrName.push_back((*iter)->GetAttrName());
        }
        ASSERT_THAT(deletedAttrName, testing::ElementsAre("string2", "price", "nid"));
        // make add index not ready
        IndexConfigIteratorPtr indexConfigs = loadSchema->GetIndexSchema()->CreateIterator(true, is_deleted);
        vector<string> deletedIndexName;
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++) {
            deletedIndexName.push_back((*iter)->GetIndexName());
        }
        ASSERT_THAT(deletedIndexName, testing::ElementsAre("index2", "nid"));
        indexConfigs = loadSchema->GetIndexSchema()->CreateIterator(true, is_disable);
        vector<string> disableIndexName;
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++) {
            disableIndexName.push_back((*iter)->GetIndexName());
        }
        ASSERT_THAT(disableIndexName, testing::ElementsAre("nid"));
    }
}

void IndexPartitionSchemaTest::TestSupportAutoUpdate()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "support_auto_update_schema.json");
    AttributeConfigIteratorPtr attrConfigs = loadSchema->GetAttributeSchema()->CreateIterator(is_deleted);
    vector<string> deletedAttrName;
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
        deletedAttrName.push_back((*iter)->GetAttrName());
    }
    ASSERT_THAT(deletedAttrName, testing::ElementsAre("string2", "price", "nid"));
    // disable not updatable attribute
    loadSchema->MarkOngoingModifyOperation(3);
    ASSERT_TRUE(loadSchema->GetAttributeSchema()->GetAttributeConfig("nid")->IsDisabled());
    ASSERT_TRUE(loadSchema->SupportAutoUpdate());
}

void IndexPartitionSchemaTest::TestCaseForMarkOngoingOperation()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_modify_operations.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->AssertEqual(*cloneSchema);

    schema->MarkOngoingModifyOperation(3);
    cout << schema->GetEffectiveIndexInfo(true) << endl;
    cout << schema->GetEffectiveIndexInfo(false) << endl;
    ASSERT_TRUE(schema->GetIndexSchema()->GetIndexConfig("nid")->IsDisabled());
    ASSERT_TRUE(schema->GetAttributeSchema()->GetAttributeConfig("nid")->IsDisabled());

    vector<schema_opid_t> notReadyIds;
    schema->GetNotReadyModifyOperationIds(notReadyIds);
    ASSERT_EQ(1, notReadyIds.size());
    ASSERT_EQ(3, notReadyIds[0]);
}

void IndexPartitionSchemaTest::TestCaseForInvalidModifyOperation()
{
    // test delete field nid still in use by index
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema1_with_modify_operations.json"),
                 util::SchemaException);

    // test delete no_exist index
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema2_with_modify_operations.json"),
                 util::SchemaException);

    // test deplicated add nid index
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema3_with_modify_operations.json"),
                 util::SchemaException);

    // test user set schemaid
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema4_with_modify_operations.json"),
                 util::SchemaException);

    // test empty modify operation
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_schema5_with_modify_operations.json"),
                 util::SchemaException);

    // test add truncate index
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "add_truncate_index_with_modify_operations.json"),
                 util::UnSupportedException);
    // test add adaptive bitmap index
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "add_adaptive_bitmap_index_with_modify_operations.json"),
                 util::UnSupportedException);
}

void IndexPartitionSchemaTest::TestCaseForAddNonVirtualException()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_modify_operations.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);

    {
        IndexPartitionSchemaPtr schema(cloneSchema->Clone());
        ASSERT_ANY_THROW(schema->AddFieldConfig("test_field", ft_int32, true));
        ASSERT_NO_THROW(schema->AddFieldConfig("test_field", ft_int32, true, true));
    }

    {
        IndexPartitionSchemaPtr schema(cloneSchema->Clone());
        FieldConfigPtr field = schema->AddFieldConfig("test_field", ft_string, true, true);
        SingleFieldIndexConfigPtr config(new SingleFieldIndexConfig("test_index", it_string));
        config->SetFieldConfig(field);
        ASSERT_ANY_THROW(schema->AddIndexConfig(config));
        config->SetVirtual(true);
        ASSERT_NO_THROW(schema->AddIndexConfig(config));
    }
}

void IndexPartitionSchemaTest::TestCaseForCompitableWithModifyOperation()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_modify_operations.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema1(new IndexPartitionSchema("noname"));
    schema1->SetMaxModifyOperationCount(1);
    schema1->EnableThrowAssertCompatibleException();
    ASSERT_EQ(1, schema1->GetMaxModifyOperationCount());
    FromJson(*schema1, comparedAny);
    ASSERT_EQ(1, schema1->GetModifyOperationCount());

    IndexPartitionSchemaPtr schema2(new IndexPartitionSchema("noname"));
    schema2->SetMaxModifyOperationCount(2);
    schema2->EnableThrowAssertCompatibleException();
    ASSERT_EQ(2, schema2->GetMaxModifyOperationCount());
    FromJson(*schema2, comparedAny);
    ASSERT_EQ(2, schema2->GetModifyOperationCount());

    IndexPartitionSchemaPtr schema3(new IndexPartitionSchema("noname"));
    schema3->SetMaxModifyOperationCount(3);
    schema3->EnableThrowAssertCompatibleException();
    ASSERT_EQ(3, schema3->GetMaxModifyOperationCount());
    FromJson(*schema3, comparedAny);
    ASSERT_EQ(3, schema3->GetModifyOperationCount());

    IndexPartitionSchemaPtr schema4(new IndexPartitionSchema("noname"));
    schema4->SetMaxModifyOperationCount(4);
    schema4->EnableThrowAssertCompatibleException();
    ASSERT_EQ(4, schema4->GetMaxModifyOperationCount());
    FromJson(*schema4, comparedAny);
    ASSERT_EQ(4, schema4->GetModifyOperationCount());

    ASSERT_NO_THROW(schema1->AssertCompatible(*schema2));
    ASSERT_NO_THROW(schema2->AssertCompatible(*schema3));
    ASSERT_NO_THROW(schema1->AssertCompatible(*schema3));
    ASSERT_NO_THROW(schema3->AssertCompatible(*schema4));

    ASSERT_ANY_THROW(schema2->AssertCompatible(*schema1));
    ASSERT_ANY_THROW(schema3->AssertCompatible(*schema2));
    ASSERT_ANY_THROW(schema3->AssertCompatible(*schema1));
}

void IndexPartitionSchemaTest::TestDisableOperation()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_disable_operation.json");
    ASSERT_EQ(4, loadSchema->GetModifyOperationCount());

    vector<schema_opid_t> disableOps;
    loadSchema->GetDisableOperations(disableOps);
    ASSERT_EQ(1, disableOps.size());
    ASSERT_EQ(2, disableOps[0]);
}

void IndexPartitionSchemaTest::TestOperationNoAdd()
{
    IndexPartitionSchemaPtr loadSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_all_delete_operation.json");
    ASSERT_EQ(1, loadSchema->GetModifyOperationCount());

    vector<schema_opid_t> disableOps;
    loadSchema->GetDisableOperations(disableOps);
    ASSERT_EQ(0, disableOps.size());
}

void IndexPartitionSchemaTest::CheckFieldSchema(const FieldSchemaPtr& schema, const string& infoStr)
{
    vector<vector<string>> fieldInfos;
    StringUtil::fromString(infoStr, fieldInfos, ":", ";");
    FieldSchema::Iterator iter = schema->Begin();
    ASSERT_EQ(fieldInfos.size(), schema->GetFieldCount());
    for (fieldid_t i = 0; i < (fieldid_t)fieldInfos.size(); i++) {
        assert(fieldInfos[i].size() == 2);
        const string& fieldName = fieldInfos[i][0];
        const string& status = fieldInfos[i][1];
        FieldConfigPtr config = *iter;
        ASSERT_EQ(i, config->GetFieldId());
        ASSERT_EQ(fieldName, config->GetFieldName());
        if (status == "delete") {
            ASSERT_TRUE(!schema->GetFieldConfig(i));
            ASSERT_TRUE(config->IsDeleted());
        } else if (status == "normal") {
            FieldConfigPtr tmp = schema->GetFieldConfig(fieldName);
            ASSERT_EQ(i, tmp->GetFieldId());
            tmp = schema->GetFieldConfig(i);
            ASSERT_EQ(fieldName, tmp->GetFieldName());
            ASSERT_TRUE(config->IsNormal());
        } else {
            assert(false);
        }
        ++iter;
    }
    ASSERT_EQ(iter, schema->End());

    FieldConfigIteratorPtr configIter = schema->CreateIterator(is_normal);
    iter = configIter->Begin();
    for (fieldid_t i = 0; i < (fieldid_t)fieldInfos.size(); i++) {
        const string& fieldName = fieldInfos[i][0];
        const string& status = fieldInfos[i][1];
        if (status != "normal") {
            continue;
        }
        FieldConfigPtr config = *iter;
        ASSERT_EQ(i, config->GetFieldId());
        ASSERT_EQ(fieldName, config->GetFieldName());
        ++iter;
    }
    ASSERT_EQ(iter, configIter->End());
}

void IndexPartitionSchemaTest::CheckIndexSchema(const IndexSchemaPtr& schema, const string& infoStr)
{
    vector<vector<string>> indexInfos;
    StringUtil::fromString(infoStr, indexInfos, ":", ";");
    IndexSchema::Iterator iter = schema->Begin();
    ASSERT_EQ(indexInfos.size(), schema->GetIndexCount());
    for (indexid_t i = 0; i < (indexid_t)indexInfos.size(); i++) {
        assert(indexInfos[i].size() == 2);
        const string& indexName = indexInfos[i][0];
        const string& status = indexInfos[i][1];
        IndexConfigPtr config = *iter;
        assert(config->GetFieldCount() == 1);

        SingleFieldIndexConfigPtr singleFieldIndexConf = std::dynamic_pointer_cast<SingleFieldIndexConfig>(config);
        fieldid_t fieldId = singleFieldIndexConf->GetFieldConfig()->GetFieldId();
        assert(fieldId != INVALID_FIELDID);

        ASSERT_EQ(i, config->GetIndexId());
        ASSERT_EQ(indexName, config->GetIndexName());
        if (status == "delete") {
            ASSERT_TRUE(!schema->GetIndexConfig(i));
            ASSERT_TRUE(config->IsDeleted());
            ASSERT_TRUE(schema->GetIndexIdList(fieldId).empty());
            ASSERT_FALSE(schema->IsInIndex(fieldId));
        } else if (status == "normal") {
            IndexConfigPtr tmp = schema->GetIndexConfig(indexName);
            ASSERT_EQ(i, tmp->GetIndexId());
            tmp = schema->GetIndexConfig(i);
            ASSERT_EQ(indexName, tmp->GetIndexName());
            ASSERT_TRUE(config->IsNormal());
            ASSERT_EQ(i, schema->GetIndexIdList(fieldId)[0]);
            ASSERT_TRUE(schema->IsInIndex(fieldId));
        } else {
            assert(false);
        }
        ++iter;
    }
    ASSERT_EQ(iter, schema->End());

    IndexConfigIteratorPtr configIter = schema->CreateIterator(is_normal);
    iter = configIter->Begin();
    for (indexid_t i = 0; i < (indexid_t)indexInfos.size(); i++) {
        const string& indexName = indexInfos[i][0];
        const string& status = indexInfos[i][1];
        if (status != "normal") {
            continue;
        }
        IndexConfigPtr config = *iter;
        ASSERT_EQ(i, config->GetIndexId());
        ASSERT_EQ(indexName, config->GetIndexName());
        ++iter;
    }
    ASSERT_EQ(iter, configIter->End());
}

void IndexPartitionSchemaTest::CheckAttributeSchema(const AttributeSchemaPtr& schema, const string& infoStr)
{
    vector<vector<string>> attributeInfos;
    StringUtil::fromString(infoStr, attributeInfos, ":", ";");
    AttributeSchema::Iterator iter = schema->Begin();
    ASSERT_EQ(attributeInfos.size(), schema->GetAttributeCount());
    for (attrid_t i = 0; i < (attrid_t)attributeInfos.size(); i++) {
        assert(attributeInfos[i].size() == 2);
        const string& attributeName = attributeInfos[i][0];
        const string& status = attributeInfos[i][1];
        AttributeConfigPtr config = *iter;
        fieldid_t fieldId = config->GetFieldConfig()->GetFieldId();

        ASSERT_EQ(i, config->GetAttrId());
        ASSERT_EQ(attributeName, config->GetAttrName());
        if (status == "delete") {
            ASSERT_TRUE(!schema->GetAttributeConfig(i));
            ASSERT_TRUE(config->IsDeleted());
            ASSERT_FALSE(schema->IsInAttribute(fieldId));
            ASSERT_FALSE(schema->GetAttributeConfigByFieldId(fieldId));
        } else if (status == "normal") {
            AttributeConfigPtr tmp = schema->GetAttributeConfig(attributeName);
            ASSERT_EQ(i, tmp->GetAttrId());
            tmp = schema->GetAttributeConfig(i);
            ASSERT_EQ(attributeName, tmp->GetAttrName());
            ASSERT_TRUE(config->IsNormal());
            ASSERT_TRUE(schema->IsInAttribute(fieldId));
        } else {
            assert(false);
        }
        ++iter;
    }
    ASSERT_EQ(iter, schema->End());

    AttributeConfigIteratorPtr configIter = schema->CreateIterator(is_normal);
    iter = configIter->Begin();
    for (attrid_t i = 0; i < (attrid_t)attributeInfos.size(); i++) {
        const string& attributeName = attributeInfos[i][0];
        const string& status = attributeInfos[i][1];
        if (status != "normal") {
            continue;
        }
        AttributeConfigPtr config = *iter;
        ASSERT_EQ(i, config->GetAttrId());
        ASSERT_EQ(attributeName, config->GetAttrName());
        ++iter;
    }
    ASSERT_EQ(iter, configIter->End());
}

void IndexPartitionSchemaTest::TestCaseForOrderPreservingField()
{
    IndexPartitionSchemaPtr normalSchema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/" + "simple_table_schema.json");
    ASSERT_EQ("ops_order_timestamp", normalSchema->GetRegionSchema(DEFAULT_REGIONID)->GetOrderPreservingFieldName());

    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/string_order_preserving.json"));

    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/no_attr_order_preserving.json"));

    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/no_field_order_preserving.json"));
}

void IndexPartitionSchemaTest::TestCaseForMultiRegionKKVSchema()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kkv_index_multi_region.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->Check();
    schema->AssertEqual(*loadSchema);
    ASSERT_EQ("kkv_table", schema->GetSchemaName());
    ASSERT_EQ(tt_kkv, schema->GetTableType());
    ASSERT_EQ((size_t)2, schema->GetRegionCount());

    {
        // region1
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("in_region");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kkv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvIndexConfig = std::dynamic_pointer_cast<KKVIndexConfig>(indexConfig);
        ASSERT_TRUE(kkvIndexConfig);

        PackAttributeConfigPtr attrConfig = kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, testing::ElementsAre(string("nick"), string("pidvid")));

        const KKVIndexPreference& indexPreference = kkvIndexConfig->GetIndexPreference();
        ASSERT_EQ(ipt_perf, indexPreference.GetType());
        const KKVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
        ASSERT_FALSE(valueParam.IsEncode());
        ASSERT_EQ(string(""), valueParam.GetFileCompressType());
        ASSERT_EQ((size_t)2, kkvIndexConfig->GetRegionCount());
        ASSERT_EQ((regionid_t)0, kkvIndexConfig->GetRegionId());
        ASSERT_TRUE(kkvIndexConfig->OptimizedStoreSKey());
        ASSERT_EQ((uint32_t)10000, kkvIndexConfig->GetSuffixKeyProtectionThreshold());
        ASSERT_EQ((uint32_t)2000, kkvIndexConfig->GetSuffixKeyTruncateLimits());
    }

    {
        // region2
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("out_region");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kkv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KKVIndexConfigPtr kkvIndexConfig = std::dynamic_pointer_cast<KKVIndexConfig>(indexConfig);
        ASSERT_TRUE(kkvIndexConfig);

        PackAttributeConfigPtr attrConfig = kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, testing::ElementsAre(string("nick"), string("nid"), string("timestamp")));

        const KKVIndexPreference& indexPreference = kkvIndexConfig->GetIndexPreference();
        ASSERT_EQ(ipt_perf, indexPreference.GetType());
        const KKVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
        ASSERT_FALSE(valueParam.IsEncode());
        ASSERT_EQ(string(""), valueParam.GetFileCompressType());
        ASSERT_EQ((size_t)2, kkvIndexConfig->GetRegionCount());
        ASSERT_EQ((regionid_t)1, kkvIndexConfig->GetRegionId());
        ASSERT_FALSE(kkvIndexConfig->OptimizedStoreSKey());
        ASSERT_EQ((uint32_t)5000, kkvIndexConfig->GetSuffixKeyProtectionThreshold());
        ASSERT_EQ((uint32_t)3000, kkvIndexConfig->GetSuffixKeyTruncateLimits());
    }
    ASSERT_TRUE(!schema->GetGlobalRegionIndexPreference().empty());
}

void IndexPartitionSchemaTest::TestCaseForMultiRegionKVSchema()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "kv_index_multi_region.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->Check();
    schema->AssertEqual(*loadSchema);
    ASSERT_EQ("kv_table", schema->GetSchemaName());
    ASSERT_EQ(tt_kv, schema->GetTableType());
    ASSERT_EQ((size_t)2, schema->GetRegionCount());

    {
        // region1
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("region1");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = std::dynamic_pointer_cast<KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, testing::ElementsAre(string("nid"), string("pidvid"), string("timestamp")));

        const KVIndexPreference& indexPreference = kvIndexConfig->GetIndexPreference();
        ASSERT_EQ(ipt_perf, indexPreference.GetType());
        const KVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
        ASSERT_FALSE(valueParam.IsEncode());
        ASSERT_EQ(string(""), valueParam.GetFileCompressType());
        ASSERT_EQ((size_t)2, kvIndexConfig->GetRegionCount());
        ASSERT_EQ((regionid_t)0, kvIndexConfig->GetRegionId());
    }

    {
        // region2
        RegionSchemaPtr regionSchema = schema->GetRegionSchema("region2");
        ASSERT_TRUE(regionSchema);

        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = std::dynamic_pointer_cast<KVIndexConfig>(indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, testing::ElementsAre(string("nid"), string("pidvid")));

        const KVIndexPreference& indexPreference = kvIndexConfig->GetIndexPreference();
        ASSERT_EQ(ipt_perf, indexPreference.GetType());
        const KVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
        ASSERT_FALSE(valueParam.IsEncode());
        ASSERT_EQ(string(""), valueParam.GetFileCompressType());
        ASSERT_EQ((size_t)2, kvIndexConfig->GetRegionCount());
        ASSERT_EQ((regionid_t)1, kvIndexConfig->GetRegionId());
    }
    ASSERT_TRUE(!schema->GetGlobalRegionIndexPreference().empty());
}

void IndexPartitionSchemaTest::TestCaseForNoIndexException()
{
    ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_no_indexs_exception.json"));
}

void IndexPartitionSchemaTest::TestCaseForCustomizedTable()
{
    IndexPartitionSchemaPtr schema =
        ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/" + "customized_table_schema.json");
    // table level
    // case 1: normal situation
    ASSERT_EQ(schema->GetTableType(), tt_customized);
    const CustomizedTableConfigPtr& customizedTableConfig = schema->GetCustomizedTableConfig();
    ASSERT_TRUE(customizedTableConfig);
    ASSERT_EQ(customizedTableConfig->GetPluginName(), "plugin1");
    string value;
    ASSERT_TRUE(customizedTableConfig->GetParameters("key", value));
    ASSERT_EQ(value, "param1");
    ASSERT_FALSE(customizedTableConfig->GetParameters("not-exist-key", value));
    ASSERT_EQ(value, "param1");
    const CustomizedConfigVector& documentConfigs = schema->GetCustomizedDocumentConfigs();
    ASSERT_EQ(documentConfigs.size(), 2);
    ASSERT_EQ(documentConfigs[0]->GetId(), config::CUSTOMIZED_DOCUMENT_CONFIG_PARSER);
    ASSERT_EQ(documentConfigs[0]->GetPluginName(), "plugin2");
    ASSERT_TRUE(documentConfigs[0]->GetParameters("key", value));
    ASSERT_EQ(value, "param2");
    ASSERT_EQ(documentConfigs[1]->GetId(), "document.raw");
    ASSERT_EQ(documentConfigs[1]->GetPluginName(), "plugin3");
    ASSERT_TRUE(documentConfigs[1]->GetParameters("key", value));
    ASSERT_EQ(value, "param3");
    // accessor and format level
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("title_index");
    const CustomizedConfigVector& customizedConfigs = indexConfig->GetCustomizedConfigs();
    ASSERT_EQ(customizedConfigs.size(), 1);
    ASSERT_EQ(customizedConfigs[0]->GetPluginName(), "plugin4");
    ASSERT_TRUE(customizedConfigs[0]->GetParameters("key", value));
    ASSERT_EQ(value, "param4");
    ASSERT_EQ(customizedConfigs[0]->GetId(), "accessor.writer");
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    AttributeConfigPtr attr2 = attrSchema->GetAttributeConfig("count");
    const CustomizedConfigVector& attr2CusConfigs = attr2->GetCustomizedConfigs();
    ASSERT_EQ(attr2CusConfigs.size(), 1);
    ASSERT_EQ(attr2CusConfigs[0]->GetPluginName(), "plugin5");
    ASSERT_TRUE(attr2CusConfigs[0]->GetParameters("key", value));
    ASSERT_EQ(value, "param5");
    ASSERT_EQ(attr2CusConfigs[0]->GetId(), "accessor.reader");

    // check to json
    Any any = ToJson(*schema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr clonedSchema(cloneSchema->Clone());
    clonedSchema->Check();
    clonedSchema->AssertEqual(*schema);
}

void IndexPartitionSchemaTest::TestErrorCaseForCustomizedTable()
{
    // table level
    {
        // case 1: not customized but with customized parts
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/normal_table_with_customized_schema.json"));
    }
    {
        // customized table without attributes and indexs
        ASSERT_NO_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_table_without_attributes_indexs_schema.json"));
    }
    {
        // case 2: customized but without customized parts
        ASSERT_ANY_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_table_without_customized_schema.json"));
    }
    {
        // case 3: customized document only
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/" + "customized_document_only_schema.json"));
    }
    {
        // case 4: customized table miss plugin name
        // without plugin name field
        ASSERT_ANY_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/" + "customized_table_miss_plugin_field_schema.json"));
        // with plugin name field
        ASSERT_ANY_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/" + "customized_table_miss_plugin_content_schema.json"));
    }
    {
        // case 5: customized document miss id
        // without id field
        ASSERT_ANY_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/" + "customized_document_miss_id_field_schema.json"));
        // with id field
        ASSERT_ANY_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/" + "customized_document_miss_id_content_schema.json"));
    }
    {
        // case 6: error parameters
        ASSERT_ANY_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_table_with_error_parameters_schema.json"));
        ASSERT_ANY_THROW(
            ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_document_with_error_parameters_schema.json"));
    }

    // field  level
    {
        // lack of id or empty id
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema1.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema2.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema3.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema4.json"));

        // lack of pluginName or empty pluginName
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema5.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema6.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema7.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema8.json"));

        // error parameters
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_field_schema9.json"));

        // kv/kkv table with customized accessor or format
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_kv_schema1.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_kv_schema2.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_kkv_schema1.json"));
        ASSERT_ANY_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema/customized_kkv_schema2.json"));
    }
}

void IndexPartitionSchemaTest::TestCaseForAddSpatialIndex()
{
    IndexPartitionSchemaPtr oldSchema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32", "numberIndex:number:field1;", "", "");

    IndexPartitionSchemaPtr newSchema(oldSchema->Clone());

    // only add spatial index
    ModifySchemaMaker::AddModifyOperations(newSchema, "", "coordinate:location", "spatial_index:spatial:coordinate",
                                           "");

    IndexConfigPtr indexConfig = newSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_TRUE(indexConfig);
    ASSERT_EQ(1, indexConfig->GetOwnerModifyOperationId());

    AttributeConfigPtr attrConfig = newSchema->GetAttributeSchema()->GetAttributeConfig("coordinate");
    ASSERT_TRUE(attrConfig);
    ASSERT_EQ(1, attrConfig->GetOwnerModifyOperationId());
    ASSERT_EQ(AttributeConfig::ct_index_accompany, attrConfig->GetConfigType());

    ModifySchemaMaker::AddModifyOperations(newSchema, "indexs=spatial_index", "", "", "");
    indexConfig = newSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_FALSE(indexConfig);
    attrConfig = newSchema->GetAttributeSchema()->GetAttributeConfig("coordinate");
    ASSERT_FALSE(attrConfig);

    // add spatial index & attribute
    ModifySchemaMaker::AddModifyOperations(newSchema, "", "", "spatial_index:spatial:coordinate", "coordinate");
    indexConfig = newSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_TRUE(indexConfig);
    ASSERT_EQ(3, indexConfig->GetOwnerModifyOperationId());

    attrConfig = newSchema->GetAttributeSchema()->GetAttributeConfig("coordinate");
    ASSERT_TRUE(attrConfig);
    ASSERT_EQ(3, attrConfig->GetOwnerModifyOperationId());
    ASSERT_EQ(AttributeConfig::ct_normal, attrConfig->GetConfigType());

    ModifySchemaMaker::AddModifyOperations(newSchema, "indexs=spatial_index", "", "", "");
    indexConfig = newSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_FALSE(indexConfig);
    attrConfig = newSchema->GetAttributeSchema()->GetAttributeConfig("coordinate");
    ASSERT_TRUE(attrConfig);
}

void IndexPartitionSchemaTest::TestDeleteTruncateIndex()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "truncate_example_with_shard.json");
    ModifySchemaMaker::AddModifyOperations(schema, "indexs=phrase", "", "", "");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigIteratorPtr iter = indexSchema->CreateIterator(true, is_deleted);
    size_t count = 0;
    for (auto it = iter->Begin(); it != iter->End(); it++) {
        cout << (*it)->GetIndexName() << endl;
        ++count;
    }
    // shard_num = 4, truncate_profile_num = 2
    // count = (4 + 1) * (2 + 1)
    ASSERT_EQ(15, count);
}

void IndexPartitionSchemaTest::TestDeleteAttribute()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "truncate_example_with_shard.json");

    ASSERT_THROW(ModifySchemaMaker::AddModifyOperations(schema, "attributes=user_id", "", "", ""),
                 util::SchemaException);
    ASSERT_THROW(ModifySchemaMaker::AddModifyOperations(schema, "attributes=price", "", "", ""), util::SchemaException);
}

void IndexPartitionSchemaTest::TestCreateIterator()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32", "numberIndex:number:field1;", "field1;field2", "");

    // add spatial_index & cooridnate as accompany
    ModifySchemaMaker::AddModifyOperations(schema, "", "coordinate:location", "spatial_index:spatial:coordinate", "");

    CheckIterator(schema, "field", is_normal, {"field1", "field2", "coordinate"});
    CheckIterator(schema, "index", is_normal, {"numberIndex", "spatial_index"});
    CheckIterator(schema, "attribute", is_normal, {"field1", "field2", "coordinate"});

    // delete spatial_index & coordinate as accompany, field1
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=field1;indexs=spatial_index", "", "", "");
    CheckIterator(schema, "field", is_normal, {"field1", "field2", "coordinate"});
    CheckIterator(schema, "index", is_normal, {"numberIndex"});
    CheckIterator(schema, "attribute", is_normal, {"field2"});
    CheckIterator(schema, "index", is_deleted, {"spatial_index"});
    CheckIterator(schema, "attribute", is_deleted, {"field1", "coordinate"});

    // delete field2
    // add spatial_index & coordinate & field3
    ModifySchemaMaker::AddModifyOperations(schema, "fields=field2;attributes=field2", "field3:string",
                                           "spatial_index:spatial:coordinate", "coordinate;field3");
    schema->MarkOngoingModifyOperation(3);

    CheckIterator(schema, "field", is_normal, {"field1", "coordinate", "field3"});
    CheckIterator(schema, "field", is_all, {"field1", "field2", "coordinate", "field3"});
    CheckIterator(schema, "field", is_deleted, {"field2"});

    CheckIterator(schema, "index", is_all, {"numberIndex", "spatial_index", "spatial_index"});
    CheckIterator(schema, "index", is_normal, {"numberIndex"});
    CheckIterator(schema, "index", is_disable, {"spatial_index"});
    CheckIterator(schema, "index", is_deleted, {"spatial_index"});

    CheckIterator(schema, "attribute", is_all, {"field1", "field2", "coordinate", "coordinate", "field3"});
    CheckIterator(schema, "attribute", is_normal, {});
    CheckIterator(schema, "attribute", is_disable, {"coordinate", "field3"});
    CheckIterator(schema, "attribute", is_deleted, {"field1", "field2", "coordinate"});
}

void IndexPartitionSchemaTest::TestEffectiveIndexInfo()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32", "numberIndex:number:field1;", "field1;field2", "");

    // add spatial_index & cooridnate as accompany
    ModifySchemaMaker::AddModifyOperations(schema, "", "coordinate:location", "spatial_index:spatial:coordinate", "");
    CheckEffectiveIndexInfo(schema, 1, {}, "numberIndex:NUMBER;spatial_index:SPATIAL",
                            "field1:LONG;field2:UINT32;coordinate:LOCATION");

    CheckEffectiveIndexInfo(schema, 1, {}, "spatial_index:SPATIAL", "coordinate:LOCATION", true);

    // delete spatial_index & coordinate as accompany, field1
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=field1;indexs=spatial_index", "", "", "");
    CheckEffectiveIndexInfo(schema, 2, {}, "numberIndex:NUMBER", "field2:UINT32");

    // delete field2
    // add spatial_index & coordinate & field3
    ModifySchemaMaker::AddModifyOperations(schema, "fields=field2;attributes=field2", "field3:string",
                                           "spatial_index:spatial:coordinate", "coordinate;field3");
    CheckEffectiveIndexInfo(schema, 3, {3}, "numberIndex:NUMBER", "");
}

void IndexPartitionSchemaTest::CheckEffectiveIndexInfo(const IndexPartitionSchemaPtr& schema, schema_opid_t opId,
                                                       const vector<schema_opid_t>& ongoingId,
                                                       const string& expectIndexs, const string& expectAttrs,
                                                       bool onlyModifyItem)
{
    IndexPartitionSchemaPtr targetSchema(schema->CreateSchemaForTargetModifyOperation(opId));
    for (const auto& id : ongoingId) {
        targetSchema->MarkOngoingModifyOperation(id);
    }
    Any any = ParseJson(targetSchema->GetEffectiveIndexInfo(onlyModifyItem));
    JsonMap jsonMap = AnyCast<JsonMap>(any);
    auto parseItem = [&jsonMap](const string& key) {
        vector<ModifyItemPtr> items;
        JsonMap::iterator iter = jsonMap.find(key);
        if (iter != jsonMap.end()) {
            FromJson(items, iter->second);
        }
        return items;
    };

    vector<ModifyItemPtr> indexItems = parseItem(INDEXS);
    vector<ModifyItemPtr> attrItems = parseItem(ATTRIBUTES);
    auto checkItems = [](const string& expectInfosStr, const vector<ModifyItemPtr>& items) {
        vector<vector<string>> expectIndexInfos;
        StringUtil::fromString(expectInfosStr, expectIndexInfos, ":", ";");
        ASSERT_EQ(items.size(), expectIndexInfos.size());
        for (size_t i = 0; i < items.size(); i++) {
            assert(expectIndexInfos[i].size() == 2);
            ASSERT_EQ(items[i]->GetName(), expectIndexInfos[i][0]);
            ASSERT_EQ(items[i]->GetType(), expectIndexInfos[i][1]);
        }
    };
    checkItems(expectIndexs, indexItems);
    checkItems(expectAttrs, attrItems);
}

void IndexPartitionSchemaTest::CheckIterator(const IndexPartitionSchemaPtr& schema, const string& target,
                                             IndexStatus type, const vector<string>& expectNames)
{
#define CHECK_ITERATOR(funcName, iter, expectNames)                                                                    \
    size_t idx = 0;                                                                                                    \
    for (auto it = iter->Begin(); it != iter->End(); it++) {                                                           \
        ASSERT_TRUE(idx < expectNames.size());                                                                         \
        ASSERT_EQ(expectNames[idx++], (*it)->funcName());                                                              \
    }                                                                                                                  \
    ASSERT_EQ(expectNames.size(), idx);

    if (target == "index") {
        auto iter = schema->GetIndexSchema()->CreateIterator(false, type);
        CHECK_ITERATOR(GetIndexName, iter, expectNames);
    } else if (target == "attribute") {
        auto iter = schema->GetAttributeSchema()->CreateIterator(type);
        CHECK_ITERATOR(GetAttrName, iter, expectNames);
    } else if (target == "field") {
        auto iter = schema->GetFieldSchema()->CreateIterator(type);
        CHECK_ITERATOR(GetFieldName, iter, expectNames);
    } else {
        assert(false);
    }

#undef CHECK_ITERATOR
}

void IndexPartitionSchemaTest::TestCaseForTimestamp()
{
    IndexPartitionSchemaPtr schema =
        SchemaMaker::MakeSchema("field1:timestamp;field2:time", "index:date:field1", "", "field1;field2");
    std::shared_ptr<SummaryConfig> summaryConfig = schema->GetSummarySchema()->GetSummaryConfig("field1");
    ASSERT_TRUE(summaryConfig);
    summaryConfig = schema->GetSummarySchema()->GetSummaryConfig("field2");
    ASSERT_TRUE(summaryConfig);

    AttributeConfigPtr attributeConfig = schema->GetAttributeSchema()->GetAttributeConfig("field1");
    ASSERT_TRUE(attributeConfig);
    attributeConfig = schema->GetAttributeSchema()->GetAttributeConfig("field2");
    ASSERT_FALSE(attributeConfig);
}

void IndexPartitionSchemaTest::TestSourceSchema()
{
    IndexPartitionSchemaPtr tmpSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_source.json");
    string jsonString = ToJsonString(tmpSchema);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    const SourceSchemaPtr& srcSchema = schema->GetSourceSchema();
    ASSERT_TRUE(srcSchema);
    ASSERT_EQ(3, srcSchema->GetSourceGroupCount());

    vector<SourceGroupConfig::SourceFieldMode> types;
    for (auto iter = srcSchema->Begin(); iter != srcSchema->End(); iter++) {
        SourceGroupConfigPtr grpConf = *iter;
        ASSERT_TRUE(grpConf);
        types.push_back(grpConf->GetFieldMode());
    }
    vector<SourceGroupConfig::SourceFieldMode> expectResult = {SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD,
                                                               SourceGroupConfig::SourceFieldMode::USER_DEFINE,
                                                               SourceGroupConfig::SourceFieldMode::ALL_FIELD};
    ASSERT_EQ(expectResult, types);
    ASSERT_NO_THROW(schema->Check());

    auto fileCompressConfig = srcSchema->GetGroupConfig(0)->GetParameter().GetFileCompressConfig();
    ASSERT_TRUE(fileCompressConfig);

    // not allow empty source group
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_empty_source.json"), util::SchemaException);

    // not allow non-existed field in source group config
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "schema_with_source_exception.json"), util::SchemaException);
}

void IndexPartitionSchemaTest::TestSchemaWithHashId()
{
    auto checkSchemaWithHashId = [](const string& schemaString, const string& expectHashIdField,
                                    bool isBuiltInField = true) {
        IndexPartitionSchemaPtr loadSchema(new IndexPartitionSchema("no_name"));
        FromJsonString(*loadSchema, schemaString);

        Any any = ToJson(*loadSchema);
        string jsonString = ToString(any);
        Any comparedAny = ParseJson(jsonString);
        IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
        FromJson(*cloneSchema, comparedAny);
        IndexPartitionSchemaPtr schema(cloneSchema->Clone());
        schema->Check();
        schema->AssertEqual(*loadSchema);

        FieldConfigPtr fieldConfig = schema->GetFieldConfig(expectHashIdField);
        ASSERT_TRUE(fieldConfig);
        ASSERT_EQ(FieldType::ft_uint16, fieldConfig->GetFieldType());
        ASSERT_FALSE(fieldConfig->IsMultiValue());
        ASSERT_EQ(expectHashIdField, fieldConfig->GetFieldName());
        ASSERT_EQ(isBuiltInField, fieldConfig->IsBuiltInField());

        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(expectHashIdField);
        ASSERT_TRUE(attrConfig);
        ASSERT_EQ(FieldType::ft_uint16, attrConfig->GetFieldType());
        ASSERT_FALSE(attrConfig->IsMultiValue());
        ASSERT_EQ(expectHashIdField, attrConfig->GetAttrName());
        ASSERT_TRUE(attrConfig->GetCompressType().HasEquivalentCompress());
        ASSERT_EQ(isBuiltInField, attrConfig->IsBuiltInAttribute());
    };

    {
        // without user-defined hashIdFieldName
        string schemaString = R"(
        {
            "attributes": [
                "string1",
                "price"
            ],
            "fields": [
                {
                    "field_name": "string1",
                    "field_type": "STRING"
                },
                {
                    "field_name": "price",
                    "field_type": "UINT32"
                }
            ],
            "indexs": [
                {
                    "has_primary_key_attribute": true,
                    "index_fields": "string1",
                    "index_name": "pk",
                    "index_type": "PRIMARYKEY64"
                }
            ],
            "table_name": "noname",
            "enable_hash_id": true
        })";
        checkSchemaWithHashId(schemaString, "_doc_hash_id_");
    }
    {
        // with user-defined hashIdFieldName which not defined in fields
        string schemaString = R"(
        {
            "attributes": [
                "string1",
                "price"
            ],
            "fields": [
                {
                    "field_name": "string1",
                    "field_type": "STRING"
                },
                {
                    "field_name": "price",
                    "field_type": "UINT32"
                }
            ],
            "indexs": [
                {
                    "has_primary_key_attribute": true,
                    "index_fields": "string1",
                    "index_name": "pk",
                    "index_type": "PRIMARYKEY64"
                }
            ],
            "table_name": "noname",
            "enable_hash_id": true,
            "hash_id_field_name" : "udf_hash_id"
        })";
        checkSchemaWithHashId(schemaString, "udf_hash_id");
    }

    {
        // with user-defined hashIdFieldName defined in fields
        string schemaString = R"(
        {
            "attributes": [
                "string1",
                "price"
            ],
            "fields": [
                {
                    "field_name": "string1",
                    "field_type": "STRING"
                },
                {
                    "field_name": "price",
                    "field_type": "UINT32"
                },
                {
                    "field_name": "udf_hash_id",
                    "field_type": "UINT16",
                    "compress_type": "equal"
                }
            ],
            "indexs": [
                {
                    "has_primary_key_attribute": true,
                    "index_fields": "string1",
                    "index_name": "pk",
                    "index_type": "PRIMARYKEY64"
                }
            ],
            "table_name": "noname",
            "enable_hash_id": true,
            "hash_id_field_name" : "udf_hash_id"
        })";
        checkSchemaWithHashId(schemaString, "udf_hash_id", false);
    }

    {
        // _doc_hash_id_ should be uint16
        string schemaString = R"(
        {
            "attributes": [
                "string1",
                "price"
            ],
            "fields": [
                {
                    "field_name": "string1",
                    "field_type": "STRING"
                },
                {
                    "field_name": "price",
                    "field_type": "UINT32"
                },
                {
                    "field_name": "_doc_hash_id_",
                    "field_type": "STRING"
                }
            ],
            "indexs": [
                {
                    "has_primary_key_attribute": true,
                    "index_fields": "string1",
                    "index_name": "pk",
                    "index_type": "PRIMARYKEY64"
                }
            ],
            "table_name": "noname",
            "enable_hash_id": true
        })";
        IndexPartitionSchemaPtr loadSchema(new IndexPartitionSchema("no_name"));
        ASSERT_THROW(FromJsonString(*loadSchema, schemaString), util::SchemaException);
    }
}

void IndexPartitionSchemaTest::TestCaseForModifyOperationWithTruncateIndex()
{
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "modify_operation_with_truncate_index.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(confFile, jsonString).Code());

    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    for (indexid_t i = 0; i < (indexid_t)indexSchema->GetIndexCount(); i++) {
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(i);
        ASSERT_TRUE(indexConfig);
        ASSERT_EQ(i, indexConfig->GetIndexId());

        string indexName = indexConfig->GetIndexName();
        ASSERT_EQ(i, indexSchema->GetIndexConfig(indexName)->GetIndexId());
        ASSERT_EQ(indexName, indexSchema->GetIndexConfig(indexName)->GetIndexName());
    }
}

void IndexPartitionSchemaTest::TestDefaultIndexFormatVersion()
{
    // read config file
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json";
    ifstream in(confFile.c_str());
    string line;
    string jsonString;
    while (getline(in, line)) {
        jsonString += line;
    }

    Any any = ParseJson(jsonString);

    // directly load, no format_version_id will use DEFAULT_FORMAT_VERSION
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("title");
    ASSERT_EQ(IndexConfig::DEFAULT_FORMAT_VERSION, indexConfig->GetIndexFormatVersionId());

    // load from index, no format_version_id will use 0
    IndexPartitionSchemaPtr rootSchema(new IndexPartitionSchema("noname"));
    rootSchema->SetLoadFromIndex(true);
    FromJson(*rootSchema, any);
    indexSchema = rootSchema->GetIndexSchema();
    indexConfig = indexSchema->GetIndexConfig("title");
    ASSERT_EQ(0, indexConfig->GetIndexFormatVersionId());
}

void IndexPartitionSchemaTest::TestCaseForIndexWithFileCompress()
{
    IndexPartitionSchemaPtr onDiskSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_with_compress_type.json");
    ASSERT_EQ("simple_table", onDiskSchema->GetSchemaName());
    onDiskSchema->SetSchemaName("noname");
    IndexPartitionSchemaPtr clonedSchema(onDiskSchema->Clone());
    ASSERT_NO_THROW(onDiskSchema->AssertEqual(*clonedSchema));

    // test jsonize
    string str = ToJsonString(onDiskSchema);
    Any jsonAny = ParseJson(str);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(onDiskSchema->GetSchemaName()));
    FromJson(*schema, jsonAny);

    ASSERT_NO_THROW(schema->AssertEqual(*onDiskSchema));
    ASSERT_NO_THROW(schema->Check());

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("title_index");
    std::shared_ptr<FileCompressConfig> compressConfig = indexConfig->GetFileCompressConfig();
    ASSERT_TRUE(compressConfig);

    ASSERT_EQ(string("compress1"), compressConfig->GetCompressName());
    ASSERT_EQ(string("zstd"), compressConfig->GetCompressType());

    auto iter = compressConfig->GetParameters().find("level");
    ASSERT_EQ(string("10"), iter->second);
}

void IndexPartitionSchemaTest::TestCaseForNewAttributeSchema()
{
    // new attribute with no compress info
    ASSERT_NO_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "new_attribute_config_2.json"));

    IndexPartitionSchemaPtr onDiskSchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "new_attribute_config.json");

    ASSERT_EQ("auction", onDiskSchema->GetSchemaName());
    onDiskSchema->SetSchemaName("noname");
    IndexPartitionSchemaPtr clonedSchema(onDiskSchema->Clone());
    ASSERT_NO_THROW(onDiskSchema->AssertEqual(*clonedSchema));

    // test jsonize
    string str = ToJsonString(onDiskSchema);
    Any jsonAny = ParseJson(str);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(onDiskSchema->GetSchemaName()));
    FromJson(*schema, jsonAny);

    ASSERT_NO_THROW(schema->AssertEqual(*onDiskSchema));
    ASSERT_NO_THROW(schema->Check());
    // test get attribute configs
    AttributeSchemaPtr attributeSchema = schema->GetAttributeSchema();
    ASSERT_TRUE(attributeSchema);
    ASSERT_EQ(size_t(10), attributeSchema->GetAttributeCount());
    ASSERT_EQ(size_t(3), attributeSchema->GetPackAttributeCount());

    ASSERT_FALSE(attributeSchema->GetPackAttributeConfig("user_id"));
    ASSERT_FALSE(attributeSchema->GetPackAttributeConfig("user_name"));
    ASSERT_TRUE(attributeSchema->GetPackAttributeConfig("product_info"));
    ASSERT_TRUE(attributeSchema->GetPackAttributeConfig("product_price"));
    ASSERT_TRUE(attributeSchema->GetPackAttributeConfig("product_price2"));

    // test get pack attribute from normal attribute
    const AttributeConfigPtr& attrConfig = attributeSchema->GetAttributeConfig("product_id");

    attrid_t valid_id = attrConfig->GetAttrId();
    attrid_t invalid_id = 10;

    ASSERT_EQ(attributeSchema->GetPackAttributeConfig("product_info")->GetPackAttrId(),
              attributeSchema->GetPackIdByAttributeId(valid_id));
    ASSERT_EQ(INVALID_PACK_ATTRID, attributeSchema->GetPackIdByAttributeId(invalid_id));

    // test pack attribute config
    {
        PackAttributeConfigPtr packAttrConfig = attributeSchema->GetPackAttributeConfig("product_info");

        ASSERT_EQ(string("product_info"), packAttrConfig->GetPackName());
        ASSERT_EQ(attrid_t(1), packAttrConfig->GetPackAttrId());
        vector<string> subAttrNames;
        packAttrConfig->GetSubAttributeNames(subAttrNames);
        ASSERT_EQ(size_t(2), subAttrNames.size());
        ASSERT_EQ(string("product_id"), subAttrNames[0]);
        ASSERT_EQ(string("category"), subAttrNames[1]);
        CompressTypeOption compType = packAttrConfig->GetCompressType();
        ASSERT_TRUE(compType.HasEquivalentCompress());
        ASSERT_FALSE(compType.HasUniqEncodeCompress());
        std::shared_ptr<FileCompressConfig> fileCompressConfig = packAttrConfig->GetFileCompressConfig();
        ASSERT_TRUE(fileCompressConfig);
        ASSERT_EQ("compress1", fileCompressConfig->GetCompressName());
        ASSERT_EQ("zstd", fileCompressConfig->GetCompressType());
        map<string, string> expectedParam {{"level", "10"}};
        ASSERT_EQ(expectedParam, fileCompressConfig->GetParameters());
    }
    {
        auto bodyAttrConfig = attributeSchema->GetAttributeConfig("body");
        ASSERT_EQ(2, bodyAttrConfig->GetAttrId());
        auto fileCompressConfig = bodyAttrConfig->GetFileCompressConfig();
        ASSERT_TRUE(fileCompressConfig);
        ASSERT_EQ("compress1", fileCompressConfig->GetCompressName());
        ASSERT_EQ("zstd", fileCompressConfig->GetCompressType());
        map<string, string> expectedParam {{"level", "10"}};
        ASSERT_EQ(expectedParam, fileCompressConfig->GetParameters());
    }
    {
        auto useridAttrConfig = attributeSchema->GetAttributeConfig("user_id");
        auto fileCompressConfig = useridAttrConfig->GetFileCompressConfig();
        ASSERT_FALSE(fileCompressConfig);
    }
}

void IndexPartitionSchemaTest::TestCaseForInvalidNewAttributeSchema()
{
    // no compress name in file compress schema
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_new_attribute_config_2.json"),
                 util::SchemaException);

    // unknow compress type
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_new_attribute_config_4.json"),
                 util::SchemaException);

    // duplicated compress name
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_new_attribute_config_5.json"),
                 util::SchemaException);
}

void IndexPartitionSchemaTest::TestDifferentDefaultVersionIdByEnv()
{
    autil::EnvUtil::setEnv("INDEXLIB_DEFAULT_INVERTED_INDEX_FORMAT_VERSION", "0");
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("title");
    ASSERT_EQ(0, indexConfig->GetIndexFormatVersionId());

    autil::EnvUtil::setEnv("INDEXLIB_DEFAULT_INVERTED_INDEX_FORMAT_VERSION", "1");
    schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json");
    indexSchema = schema->GetIndexSchema();
    indexConfig = indexSchema->GetIndexConfig("title");
    ASSERT_EQ(1, indexConfig->GetIndexFormatVersionId());

    autil::EnvUtil::setEnv("INDEXLIB_DEFAULT_INVERTED_INDEX_FORMAT_VERSION", "10");
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json"), util::BadParameterException);

    autil::EnvUtil::unsetEnv("INDEXLIB_DEFAULT_INVERTED_INDEX_FORMAT_VERSION");
}

void IndexPartitionSchemaTest::TestCaseForTemperatureLayer()
{
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(confFile, jsonString).Code());

    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    ASSERT_TRUE(schema->EnableTemperatureLayer());
}

void IndexPartitionSchemaTest::TestCaseForTemperatureLayerWithFileCompress()
{
    // temperature compressor not exist
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid1_file_compress_schema_with_temperature.json"),
                 util::SchemaException);

    // temperature compressor has inner temperature compressor
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid2_file_compress_schema_with_temperature.json"),
                 util::SchemaException);

    // invalid temperature layer string (only support hot/warm/cold)
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid3_file_compress_schema_with_temperature.json"),
                 util::SchemaException);

    // invalid temperature layer string (only support hot/warm/cold)
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid4_file_compress_schema_with_temperature.json"),
                 util::SchemaException);

    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "file_compress_schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(confFile, jsonString).Code());

    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    /*
    "file_compress": [
        {
            "name":"compress1",
            "type":"zstd"
        },
        {
            "name":"compress2",
            "type":"snappy"
        },
        {
            "name":"hybrid_compressor",
            "type":"lz4",
            "temperature_compressor":
            {
                "WARM" : "compress1",
                "COLD"  : "compress2"
            }
        }
    ]
    */

    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("status");
    std::shared_ptr<FileCompressConfig> compressConfig = indexConfig->GetFileCompressConfig();
    ASSERT_TRUE(compressConfig);
    ASSERT_EQ(string("hybrid_compressor"), compressConfig->GetCompressName());
    ASSERT_EQ(string("lz4"), compressConfig->GetCompressType());
    ASSERT_EQ(string("lz4"), compressConfig->GetCompressType("HOT"));
    ASSERT_EQ(string("zstd"), compressConfig->GetCompressType("warm"));
    ASSERT_EQ(string("snappy"), compressConfig->GetCompressType("COLD"));

    ASSERT_EQ(4096, compressConfig->GetCompressBufferSize("HOT"));
    ASSERT_EQ(4096, compressConfig->GetCompressBufferSize("warm"));
    ASSERT_EQ(4096, compressConfig->GetCompressBufferSize("cold"));
    ASSERT_EQ(string(".*/(dictionary|posting)$"), compressConfig->GetExcludePattern("cold"));

    indexConfig = schema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_EQ(string("hybrid_compressor"), indexConfig->GetFileCompressConfig()->GetCompressName());

    AttributeConfigPtr attrConfig = schema->GetAttributeSchema()->GetAttributeConfig("location");
    ASSERT_EQ(string("hybrid_compressor"), attrConfig->GetFileCompressConfig()->GetCompressName());

    indexConfig = schema->GetIndexSchema()->GetIndexConfig("nid");
    ASSERT_EQ(string("hybrid_compressor"), indexConfig->GetFileCompressConfig()->GetCompressName());

    attrConfig = schema->GetAttributeSchema()->GetAttributeConfig("nid");
    ASSERT_EQ(string("hybrid_compressor"), attrConfig->GetFileCompressConfig()->GetCompressName());
}

void IndexPartitionSchemaTest::TestCaseForUpdateFileCompressSchema()
{
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "file_compress_schema_with_temperature.json";
    string jsonString;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(confFile, jsonString).Code());

    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    IndexConfigPtr indexConfig = schema->GetIndexSchema()->GetIndexConfig("status");
    std::shared_ptr<FileCompressConfig> compressConfig = indexConfig->GetFileCompressConfig();
    ASSERT_TRUE(compressConfig);
    ASSERT_EQ(string("hybrid_compressor"), compressConfig->GetCompressName());
    ASSERT_EQ(string("lz4"), compressConfig->GetCompressType());
    ASSERT_EQ(string("lz4"), compressConfig->GetCompressType("HOT"));
    ASSERT_EQ(string("zstd"), compressConfig->GetCompressType("warm"));
    ASSERT_EQ(string("snappy"), compressConfig->GetCompressType("COLD"));

    confFile = GET_PRIVATE_TEST_DATA_PATH() + "file_compress_schema_with_temperature_patch.json";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(confFile, jsonString).Code());
    any = ParseJson(jsonString);
    UpdateableSchemaStandardsPtr patchStandards(new UpdateableSchemaStandards);
    FromJson(*patchStandards, any);

    ASSERT_TRUE(patchStandards->GetTemperatureLayerConfig() == nullptr);
    ASSERT_TRUE(patchStandards->GetFileCompressSchema() != nullptr);

    schema->SetUpdateableSchemaStandards(*patchStandards);
    indexConfig = schema->GetIndexSchema()->GetIndexConfig("status");
    compressConfig = indexConfig->GetFileCompressConfig();
    ASSERT_TRUE(compressConfig);
    ASSERT_EQ(string("hybrid_compressor"), compressConfig->GetCompressName());
    ASSERT_EQ(string(""), compressConfig->GetCompressType());
    ASSERT_EQ(string(""), compressConfig->GetCompressType("HOT"));
    ASSERT_EQ(string("zstd"), compressConfig->GetCompressType("warm"));
    ASSERT_EQ(string("lz4"), compressConfig->GetCompressType("COLD"));
}

void IndexPartitionSchemaTest::TestCaseForNewAttributeLegacyWrite()
{
    autil::EnvGuard envGurad("INDEXLIB_FORCE_USE_LEGACY_ATTRIBUTE_CONFIG", "");
    ASSERT_TRUE(getenv("INDEXLIB_FORCE_USE_LEGACY_ATTRIBUTE_CONFIG"));
    for (auto schemaFile : {"schema.json", "newSchemaWithoutCompress.json"}) {
        auto legacySchema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + schemaFile);
        Any any = ToJson(legacySchema);
        JsonMap jsonMap = AnyCast<JsonMap>(any);
        auto iter = jsonMap.find(ATTRIBUTES);
        JsonArray attrs = AnyCast<JsonArray>(iter->second);
        for (JsonArray::iterator iter = attrs.begin(); iter != attrs.end(); ++iter) {
            ASSERT_EQ(typeid(string), iter->GetType());
        }
    }
}

void IndexPartitionSchemaTest::TestCaseForOrc()
{
    IndexPartitionSchemaPtr schema = ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "orc_schema.json");
    ASSERT_EQ(schema->GetTableType(), tt_orc);
}

void IndexPartitionSchemaTest::TestCaseForNoIndexKv()
{
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_kv_schema.json"), util::SchemaException);
    ASSERT_THROW(ReadSchema(GET_PRIVATE_TEST_DATA_PATH() + "invalid_kkv_pk_schema.json"), util::SchemaException);
}

}} // namespace indexlib::config

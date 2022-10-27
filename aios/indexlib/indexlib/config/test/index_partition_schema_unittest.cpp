#include <fstream>
#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/misc/exception.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/test/schema_loader.h"
#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/test/truncate_config_maker.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/virtual_attribute_config_creator.h"
#include "indexlib/config/test/index_partition_schema_unittest.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/kv_index_config_impl.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/modify_schema_maker.h"

using namespace std;
using namespace std::tr1;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(config);

void IndexPartitionSchemaTest::CaseSetUp()
{
}

void IndexPartitionSchemaTest::CaseTearDown()
{
}

void IndexPartitionSchemaTest::TestCaseForJsonize()
{
    // read config file
    string confFile = string(TEST_DATA_PATH) + "index_engine_example.json";
    ifstream in(confFile.c_str());
    string line;
    string jsonString;
    while(getline(in, line))
    {
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
    try
    {
        schema->AssertEqual(*comparedSchema);
    }catch(const misc::ExceptionBase& e)
    {
        exception = true;
    }
    INDEXLIB_TEST_TRUE(!exception);
}

void IndexPartitionSchemaTest::TestCaseForAddVirtualAttributeConfig()
{
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH) + "index_engine_example.json");
    vector<AttributeConfigPtr> attrConfigs;
    INDEXLIB_TEST_TRUE(!schema->AddVirtualAttributeConfigs(attrConfigs));
    AttributeSchemaPtr virtualAttrSchema = schema->GetVirtualAttributeSchema();
    INDEXLIB_TEST_TRUE(!virtualAttrSchema);

    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "0"));
    INDEXLIB_TEST_TRUE(schema->AddVirtualAttributeConfigs(attrConfigs));
    virtualAttrSchema = schema->GetVirtualAttributeSchema();
    INDEXLIB_TEST_TRUE(virtualAttrSchema);
    INDEXLIB_TEST_EQUAL((size_t)1, virtualAttrSchema->GetAttributeCount());

    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "0"));
    INDEXLIB_TEST_TRUE(!schema->AddVirtualAttributeConfigs(attrConfigs));
    virtualAttrSchema = schema->GetVirtualAttributeSchema();
    INDEXLIB_TEST_TRUE(virtualAttrSchema);
    INDEXLIB_TEST_EQUAL((size_t)1, virtualAttrSchema->GetAttributeCount());

    attrConfigs.push_back(VirtualAttributeConfigCreator::Create("vir_attr2", ft_int32, false, "0"));
    INDEXLIB_TEST_TRUE(schema->AddVirtualAttributeConfigs(attrConfigs));
    virtualAttrSchema = schema->GetVirtualAttributeSchema();
    INDEXLIB_TEST_TRUE(virtualAttrSchema);
    INDEXLIB_TEST_EQUAL((size_t)2, virtualAttrSchema->GetAttributeCount());
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

    INDEXLIB_TEST_TRUE(!schema->GetVirtualAttributeSchema());
    INDEXLIB_TEST_TRUE(!schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema());

    vector<AttributeConfigPtr> mainVirtualAttrs;
    mainVirtualAttrs.push_back(VirtualAttributeConfigCreator::Create("vir_attr1", ft_int32, false, "0"));
    INDEXLIB_TEST_TRUE(other->AddVirtualAttributeConfigs(mainVirtualAttrs));

    schema->CloneVirtualAttributes(*other);
    INDEXLIB_TEST_TRUE(schema->GetVirtualAttributeSchema());
    INDEXLIB_TEST_EQUAL(size_t(1), schema->GetVirtualAttributeSchema()->GetAttributeCount());
    INDEXLIB_TEST_TRUE(!schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema());

    vector<AttributeConfigPtr> subVirtualAttrs;
    subVirtualAttrs.push_back(VirtualAttributeConfigCreator::Create("vir_attr2", ft_int32, false, "0"));
    INDEXLIB_TEST_TRUE(other->GetSubIndexPartitionSchema()->AddVirtualAttributeConfigs(subVirtualAttrs));

    const RegionSchemaImplPtr& regionSchema = schema->GetRegionSchema(DEFAULT_REGIONID)->GetImpl();
    regionSchema->mVirtualAttributeSchema.reset();
    schema->CloneVirtualAttributes(*other);
    INDEXLIB_TEST_TRUE(schema->GetVirtualAttributeSchema());
    INDEXLIB_TEST_EQUAL(size_t(1), schema->GetVirtualAttributeSchema()->GetAttributeCount());
    INDEXLIB_TEST_TRUE(schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema());
    INDEXLIB_TEST_EQUAL(size_t(1), schema->GetSubIndexPartitionSchema()->GetVirtualAttributeSchema()->GetAttributeCount());
}

void IndexPartitionSchemaTest::TestCaseForFieldConfig()
{
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH) + "index_engine_example.json");

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    fieldid_t fieldId = fieldSchema->GetFieldConfig("title")->GetFieldId();
    INDEXLIB_TEST_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = fieldSchema->GetFieldConfig("body")->GetFieldId();
    INDEXLIB_TEST_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = fieldSchema->GetFieldConfig("user_name")->GetFieldId();
    INDEXLIB_TEST_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = fieldSchema->GetFieldConfig("product_id")->GetFieldId();
    INDEXLIB_TEST_TRUE(indexSchema->IsInIndex(fieldId));
    FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig("price2");
    INDEXLIB_TEST_TRUE(fieldConfig->GetCompressType().HasPatchCompress());
    fieldConfig = fieldSchema->GetFieldConfig("price3");
    INDEXLIB_TEST_TRUE(fieldConfig->GetCompressType().HasPatchCompress());

    // no fileds
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH)+"/schema/schema_without_fields.json"), SchemaException);
}

void IndexPartitionSchemaTest::TestCaseForGetIndexIdList()
{
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH) + "index_engine_example.json");

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema();
    fieldid_t fieldId = fieldSchema->GetFieldConfig("title")->GetFieldId();

    INDEXLIB_TEST_EQUAL((size_t)3, indexSchema->GetIndexIdList(fieldId).size());
    INDEXLIB_TEST_EQUAL((indexid_t)0, indexSchema->GetIndexIdList(fieldId)[0]);
    INDEXLIB_TEST_EQUAL((indexid_t)1, indexSchema->GetIndexIdList(fieldId)[1]);
    INDEXLIB_TEST_EQUAL((indexid_t)4, indexSchema->GetIndexIdList(fieldId)[2]);

    fieldId = fieldSchema->GetFieldConfig("body")->GetFieldId();
    INDEXLIB_TEST_EQUAL((size_t)2, indexSchema->GetIndexIdList(fieldId).size());
    INDEXLIB_TEST_EQUAL((indexid_t)0, indexSchema->GetIndexIdList(fieldId)[0]);
    INDEXLIB_TEST_EQUAL((indexid_t)4, indexSchema->GetIndexIdList(fieldId)[1]);

    fieldId = fieldSchema->GetFieldConfig("user_name")->GetFieldId();
    INDEXLIB_TEST_EQUAL((size_t)1, indexSchema->GetIndexIdList(fieldId).size());
    INDEXLIB_TEST_EQUAL((indexid_t)2, indexSchema->GetIndexIdList(fieldId)[0]);

    INDEXLIB_TEST_TRUE(indexSchema->IsInIndex(fieldId));
    fieldId = fieldSchema->GetFieldConfig("product_id")->GetFieldId();
    INDEXLIB_TEST_EQUAL((size_t)1, indexSchema->GetIndexIdList(fieldId).size());
    INDEXLIB_TEST_EQUAL((indexid_t)3, indexSchema->GetIndexIdList(fieldId)[0]);
}

void IndexPartitionSchemaTest::TestCaseForDictSchema()
{
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH) + "index_engine_example.json");
    DictionarySchemaPtr dictShema = schema->GetDictSchema();
    INDEXLIB_TEST_TRUE(dictShema != NULL);

    DictionaryConfigPtr  top10DictConfig =
        dictShema->GetDictionaryConfig("top10");
    INDEXLIB_TEST_TRUE(top10DictConfig != NULL);
    DictionaryConfigPtr top100DictConfig =
        dictShema->GetDictionaryConfig("top100");
    INDEXLIB_TEST_TRUE(top100DictConfig != NULL);

    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    INDEXLIB_TEST_TRUE(indexSchema != NULL);
    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("phrase");
    INDEXLIB_TEST_TRUE(indexConfig != NULL);
    DictionaryConfigPtr phraseDictConfig = indexConfig->GetDictConfig();
    ASSERT_NO_THROW(phraseDictConfig->AssertEqual(*top10DictConfig));

    HighFrequencyTermPostingType postingType =
        indexConfig->GetHighFrequencyTermPostingType();
    INDEXLIB_TEST_EQUAL(hp_both, postingType);
}

void IndexPartitionSchemaTest::TestCaseForCheck()
{
    // read config file
    string confFile = string(TEST_DATA_PATH) + "index_engine_example.json";
    string jsonString;
    FileSystemWrapper::AtomicLoad(confFile, jsonString);

    Any any = ParseJson(jsonString);

    // same field with 2 single field index
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
        IndexConfigPtr indexConfig =
            IndexConfigCreator::CreateSingleFieldIndexConfig("user_name2",
                    it_string, "user_name", "",  false,
                    schema->GetFieldSchema());
        schema->AddIndexConfig(indexConfig);
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    // same field with 2 single field index
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        IndexConfigPtr indexConfig =
            IndexConfigCreator::CreateSingleFieldIndexConfig(
                    "product_id2", it_primarykey64, "product_id", "",
                    false, schema->GetFieldSchema());
        ASSERT_THROW(
                schema->AddIndexConfig(indexConfig),
                SchemaException);
    }

    // wrong field order in pack
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        PackageIndexConfigPtr packConfig = dynamic_pointer_cast<PackageIndexConfig>(
                schema->GetIndexSchema()->GetIndexConfig("phrase"));
        schema->Check();
        packConfig->AddFieldConfig("taobao_body2");
        schema->Check();
        packConfig->AddFieldConfig("taobao_body1");
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    // wrong field order in expack
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        PackageIndexConfigPtr packConfig = dynamic_pointer_cast<PackageIndexConfig>(
                schema->GetIndexSchema()->GetIndexConfig("phrase2"));
        schema->Check();
        packConfig->AddFieldConfig("taobao_body2");
        schema->Check();
        packConfig->AddFieldConfig("taobao_body1");
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    // upsupport attribute
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
        schema->AddFieldConfig("TestField1", ft_text);
        ASSERT_THROW(schema->AddAttributeConfig("TestField1"), SchemaException);

        schema->AddFieldConfig("TestField2", ft_enum);
        ASSERT_THROW(schema->AddAttributeConfig("TestField2"), SchemaException);

        schema->AddFieldConfig("TestField3", ft_time);
        ASSERT_THROW(schema->AddAttributeConfig("TestField3"), SchemaException);

        schema->AddFieldConfig("TestField6", ft_online);
        ASSERT_THROW(schema->AddAttributeConfig("TestField6"), SchemaException);

        schema->AddFieldConfig("TestField7", ft_property);
        ASSERT_THROW(schema->AddAttributeConfig("TestField7"), SchemaException);

        schema->AddFieldConfig("TestField8", ft_hash_64, true);
        ASSERT_THROW(schema->AddAttributeConfig("TestField8"), SchemaException);

        schema->AddFieldConfig("TestField9", ft_hash_128, true);
        ASSERT_THROW(schema->AddAttributeConfig("TestField9"), SchemaException);
    }

    // check option flag
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);

        IndexConfigPtr indexConfig =
            schema->GetIndexSchema()->GetIndexConfig("phrase");

        indexConfig->SetOptionFlag(of_position_list);
        schema->Check();

        indexConfig->SetOptionFlag(of_position_payload);
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    // check truncate
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_NO_THROW(FromJson(*schema, any));
        TruncateProfileConfigPtr config =
            schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_product_id");
        SortParams& sortParams = const_cast<SortParams&>(config->GetTruncateSortParams());

        sortParams[0].SetSortField("price");
        ASSERT_THROW(schema->Check(), SchemaException);
        sortParams[0].SetSortField("DOC_PAYLOAD");
        schema->Check();
        sortParams[0].SetSortField("price1");
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    //check sub schema duplicated fields
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                "text1:text;text2:text", //Field schema
                "pk:PRIMARYKEY64:text2",//index schema
                "",//attribute schema
                "");//Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
            "text1:text;text3:text", "subPk:PRIMARYKEY64:text3", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    //check sub schema duplicated index
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                "text1:text;text2:text", //Field schema
                "pk:PRIMARYKEY64:text2;pack1:pack:text1;",//index schema
                "",//attribute schema
                "");//Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
            "text3:text;text4:text;", "pack1:pack:text3;subPk:PRIMARYKEY64:text4", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    //check main schema has no primary key with sub schema
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                "text1:text;", //Field schema
                "pack:pack:text1;",//index schema
                "",//attribute schema
                "");//Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
            "text2:text;text3:text", "pack1:pack:text2;pk:PRIMARYKEY64:text3;", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    //check sub schema has no primary key
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                "text1:text;text3:text", //Field schema
                "pk:PRIMARYKEY64:text3;pack:pack:text1;",//index schema
                "",//attribute schema
                "");//Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
            "text2:text;", "pack1:pack:text2;", "", "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), SchemaException);
    }

    //check sub schema check fail
    {
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(schema,
                "text1:text;text2:text", //Field schema
                "pk:PRIMARYKEY64:text2",//index schema
                "",//attribute schema
                "");//Summary schema

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
                "text3:text;text4:text",
                "subPk:PRIMARYKEY64:text3;textIndex1:text:text4;textIndex2:text:text4",
                "",
                "");
        schema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_THROW(schema->Check(), SchemaException);
    }
}

void IndexPartitionSchemaTest::TestCaseFor32FieldCountInPack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 33;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;
    try
    {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < 32; ++i)
        {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }
        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
                "TestForPack32", it_pack, fieldNames, boosts, "",
                false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    }
    catch (const SchemaException& e)
    {
        catched = true;
    }
    INDEXLIB_TEST_TRUE(!catched);
}

void IndexPartitionSchemaTest::TestCaseFor33FieldCountInPack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 33;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;

    try
    {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < fieldCount; ++i)
        {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }

        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
                "TestForPack33", it_pack, fieldNames, boosts, "",
                false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    }
    catch (const SchemaException& e)
    {
        catched = true;
    }
    INDEXLIB_TEST_TRUE(catched);
}

void IndexPartitionSchemaTest::TestCaseFor9FieldCountInExpack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 9;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;
    try
    {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < fieldCount; ++i)
        {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }
        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
                "TestForExpack9", it_expack, fieldNames, boosts, "",
                false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    }
    catch (const SchemaException& e)
    {
        catched = true;
    }
    INDEXLIB_TEST_TRUE(catched);
}

void IndexPartitionSchemaTest::TestCaseFor8FieldCountInExpack()
{
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    uint32_t fieldCount = 9;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
        stringstream ss;
        ss << i;
        FieldConfigPtr field = schema->AddFieldConfig(ss.str(), ft_text);
    }

    bool catched = false;
    try
    {
        vector<string> fieldNames;
        vector<int32_t> boosts;
        for (uint32_t i = 0; i < 8; ++i)
        {
            stringstream ss;
            ss << i;
            fieldNames.push_back(ss.str());
            boosts.push_back((int32_t)i);
        }

        PackageIndexConfigPtr packIndex = IndexConfigCreator::CreatePackageIndexConfig(
                "TestForExpack8", it_expack, fieldNames, boosts, "",
                false, schema->GetFieldSchema());
        schema->AddIndexConfig(packIndex);
    }
    catch (const SchemaException& e)
    {
        catched = true;
    }
    INDEXLIB_TEST_TRUE(!catched);
}

void IndexPartitionSchemaTest::TestCaseForJsonizeWithSubSchema()
{
    string confFile = string(TEST_DATA_PATH) + "index_engine_example_with_sub_schema.json";
    string jsonString;
    FileSystemWrapper::AtomicLoad(confFile, jsonString);
    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    schema->Check();

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    ASSERT_TRUE(subSchema);
    ASSERT_TRUE(subSchema->GetFieldSchema()->GetFieldConfig("sub_title"));

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
    string confFile = string(TEST_DATA_PATH) + "index_engine_example_with_truncate.json";
    string jsonString;
    FileSystemWrapper::AtomicLoad(confFile, jsonString);
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
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH) + "index_engine_example_with_truncate.json");
    ASSERT_NO_THROW(schema->Check());
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("phrase")->HasTruncate());
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("phrase")->HasTruncateProfile("desc_product_id"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("phrase")->HasTruncateProfile("desc_user_name"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("phrase_desc_product_id"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("phrase_desc_product_id")->IsVirtual());
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("phrase_desc_user_name"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("phrase_desc_user_name")->IsVirtual());

    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("title")->HasTruncate());
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("title")->HasTruncateProfile("desc_product_id"));
    INDEXLIB_TEST_TRUE(!indexSchema->GetIndexConfig("title")->HasTruncateProfile("desc_user_name"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("title_desc_product_id"));
    INDEXLIB_TEST_TRUE(!indexSchema->GetIndexConfig("title_desc_user_name"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("title_desc_product_id")->IsVirtual());

    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("user_name")->HasTruncate());
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("user_name")->HasTruncateProfile("desc_product_id"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("user_name")->HasTruncateProfile("desc_user_name"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("user_name_desc_product_id"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("user_name_desc_product_id")->IsVirtual());
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("user_name_desc_user_name"));
    INDEXLIB_TEST_TRUE(indexSchema->GetIndexConfig("user_name_desc_user_name")->IsVirtual());

    INDEXLIB_TEST_TRUE(!indexSchema->GetIndexConfig("product_id")->HasTruncate());
    INDEXLIB_TEST_TRUE(!indexSchema->GetIndexConfig("phrase2")->HasTruncate());
    INDEXLIB_TEST_TRUE(!indexSchema->GetIndexConfig("categoryp")->HasTruncate());
    INDEXLIB_TEST_TRUE(!indexSchema->GetIndexConfig("catmap")->HasTruncate());

    // truncate not support single compressed float
    ASSERT_ANY_THROW(ReadSchema(string(TEST_DATA_PATH) + "truncate_with_compressed_float.json"));
    ASSERT_ANY_THROW(ReadSchema(string(TEST_DATA_PATH) + "truncate_with_fp8.json"));
}

void IndexPartitionSchemaTest::TestCaseForTruncateCheck()
{
    {
        IndexPartitionSchemaPtr schema = ReadSchema(
            string(TEST_DATA_PATH) +
            "index_engine_example_with_truncate.json");

        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
        const IndexConfigPtr& indexConfig = indexSchema->GetIndexConfig("title");
        indexConfig->SetUseTruncateProfiles("bad_profile");
        ASSERT_THROW(schema->Check(), SchemaException);

    }
    {
        IndexPartitionSchemaPtr schema = ReadSchema(
            string(TEST_DATA_PATH) +
            "index_engine_example_with_truncate.json");

        TruncateProfileConfigPtr config =
            schema->GetTruncateProfileSchema()->GetTruncateProfileConfig("desc_product_id");
        SortParams& sortParams = const_cast<SortParams&>(config->GetTruncateSortParams());


        sortParams[0].SetSortField("non-exist-field");
        ASSERT_THROW(schema->Check(), SchemaException);

        sortParams[0].SetSortField("non-corresponding-attribute-field");
        ASSERT_THROW(schema->Check(), SchemaException);
    }
    {
        // truncate sort field name appear in pack attribute
        ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "pack_attribute/"
                                + "pack_attribute_schema_with_truncate_error.json"),
                     SchemaException);
    }

}

IndexPartitionSchemaPtr IndexPartitionSchemaTest::ReadSchema(const string& fileName)
{
    // read config file
    ifstream in(fileName.c_str());
    string line;
    string jsonString;
    while(getline(in, line))
    {
        jsonString += line;
    }

    Any any = ParseJson(jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);
    return schema;
}

void IndexPartitionSchemaTest::TestAssertCompatible()
{
    // check sub schema compatible

    IndexPartitionSchemaPtr schema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(schema,
            "title:text;", //Field schema
            "title:text:title",//index schema
            "",//attribute schema
            "");//Summary schema
    {
        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        ASSERT_NO_THROW(schema->AssertCompatible(*otherSchema));
    }
    {
        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
                "sub_title:text;", "sub_title:text:sub_title", "", "");
        otherSchema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_NO_THROW(schema->AssertCompatible(*otherSchema));
    }
    {
        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        IndexPartitionSchemaPtr tempSchema(schema->Clone());

        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
                "sub_title:text;", "sub_title:text:sub_title", "", "");
        tempSchema->SetSubIndexPartitionSchema(subSchema);

        const IndexPartitionSchemaImplPtr& tempImpl = tempSchema->GetImpl();
        const IndexPartitionSchemaImplPtr& otherImpl = otherSchema->GetImpl();
        ASSERT_THROW(
            tempImpl->DoAssertCompatible(*(otherImpl.get())),
            AssertCompatibleException);
    }
    {
        IndexPartitionSchemaPtr tempSchema(schema->Clone());
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
                "sub_title:text;", "sub_title:text:sub_title", "", "");
        tempSchema->SetSubIndexPartitionSchema(subSchema);

        IndexPartitionSchemaPtr otherSchema(schema->Clone());
        IndexPartitionSchemaPtr otherSubSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(otherSubSchema,
                "sub_title:text;", "sub_title2:text:sub_title", "", "");
        subSchema->SetSubIndexPartitionSchema(otherSubSchema);

        const IndexPartitionSchemaImplPtr& tempImpl = tempSchema->GetImpl();
        const IndexPartitionSchemaImplPtr& otherImpl = otherSchema->GetImpl();
        ASSERT_THROW(
            tempImpl->DoAssertCompatible(*(otherImpl.get())),
            AssertCompatibleException);
    }
}

void IndexPartitionSchemaTest::TestCaseForIsUsefulField()
{
    IndexPartitionSchemaPtr tempSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(tempSchema,
            "title:text;useless_field:string;",
            "title:text:title", "", "");
    {
        ASSERT_TRUE(tempSchema->IsUsefulField("title"));
        ASSERT_FALSE(tempSchema->IsUsefulField("useless_field"));
        ASSERT_FALSE(tempSchema->IsUsefulField("no_such_field"));
    }
    {
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema,
                "sub_title:text;useless_sub_field:string;", "sub_title:text:sub_title",
                "", "");
        tempSchema->SetSubIndexPartitionSchema(subSchema);
        ASSERT_TRUE(tempSchema->IsUsefulField("title"));
        ASSERT_TRUE(tempSchema->IsUsefulField("sub_title"));
        ASSERT_FALSE(tempSchema->IsUsefulField("useless_sub_field"));
        ASSERT_FALSE(tempSchema->IsUsefulField("no_such_field"));
    }
}

void IndexPartitionSchemaTest::TestCaseForVirtualField()
{
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH)
            + "index_engine_example.json");
    schema->Check();
    schema->AddFieldConfig("virtual_field", ft_int32, false, true);
    schema->AddAttributeConfig("virtual_field");

    string schemaString = ToJsonString(*schema);
    IndexPartitionSchemaPtr loadSchema(new IndexPartitionSchema);
    FromJsonString(*loadSchema, schemaString);

    FieldConfigPtr loadFieldConfig = loadSchema->GetFieldSchema()->GetFieldConfig("virtual_field");
    ASSERT_TRUE(loadFieldConfig->IsVirtual());
    AttributeConfigPtr loadAttributeConfig =
        loadSchema->GetAttributeSchema()->GetAttributeConfig("virtual_field");
    ASSERT_TRUE(loadAttributeConfig);
}

void IndexPartitionSchemaTest::TestCaseForVirtualAttribute()
{
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH)
            + "index_engine_example.json");
    ASSERT_TRUE(schema->GetAttributeSchema()->GetAttributeConfig("price2"));
    uint32_t fieldCount = schema->GetFieldSchema()->GetFieldCount();

    FieldConfigPtr fieldConfig(new FieldConfig(
                    "virtual_attribute", ft_int32, false));
    AttributeConfigPtr attrConfig(new AttributeConfig(
                    AttributeConfig::ct_virtual));
    attrConfig->Init(fieldConfig, common::AttributeValueInitializerCreatorPtr());

    // add virtual attribute
    ASSERT_NO_THROW(schema->AddVirtualAttributeConfig(attrConfig));

    // name already used in attribute
    fieldConfig->SetFieldName("price2");
    ASSERT_THROW(schema->AddVirtualAttributeConfig(attrConfig), SchemaException);

    // name already used in virtual attribute
    fieldConfig->SetFieldName("virtual_attribute");
    ASSERT_THROW(schema->AddVirtualAttributeConfig(attrConfig), SchemaException);

    // add another virtual attribute
    fieldConfig->SetFieldName("virtual_attribute_1");
    ASSERT_NO_THROW(schema->AddVirtualAttributeConfig(attrConfig));

    // check schema
    const AttributeSchemaPtr& virtualAttrSchema =
        schema->GetVirtualAttributeSchema();
    ASSERT_TRUE(virtualAttrSchema);
    CheckVirtualAttribute(virtualAttrSchema, "virtual_attribute", fieldCount);
    CheckVirtualAttribute(virtualAttrSchema, "virtual_attribute_1", fieldCount + 1);
}

void IndexPartitionSchemaTest::TestCaseForPackAttribute()
{
    IndexPartitionSchemaPtr onDiskSchema = ReadSchema(string(TEST_DATA_PATH)
            + "index_engine_example_with_pack_attribute.json");

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
    const AttributeConfigPtr& attrConfig =
        attributeSchema->GetAttributeConfig("product_id");

    attrid_t valid_id = attrConfig->GetAttrId();
    attrid_t invalid_id = 10;

    ASSERT_EQ(attributeSchema->GetPackAttributeConfig("product_info")->GetPackAttrId(),
              attributeSchema->GetPackIdByAttributeId(valid_id));
    ASSERT_EQ(INVALID_PACK_ATTRID,
              attributeSchema->GetPackIdByAttributeId(invalid_id));

    // test pack attribute config

    PackAttributeConfigPtr packAttrConfig =
        attributeSchema->GetPackAttributeConfig("product_info");

    ASSERT_EQ(string("product_info"), packAttrConfig->GetAttrName());
    ASSERT_EQ(attrid_t(0), packAttrConfig->GetPackAttrId());
    vector<string> subAttrNames;
    packAttrConfig->GetSubAttributeNames(subAttrNames);
    ASSERT_EQ(size_t(2), subAttrNames.size());
    ASSERT_EQ(string("product_id"), subAttrNames[0]);
    ASSERT_EQ(string("category"), subAttrNames[1]);
    CompressTypeOption compType = packAttrConfig->GetCompressType();
    ASSERT_TRUE(compType.HasEquivalentCompress());
    ASSERT_FALSE(compType.HasUniqEncodeCompress());

    packAttrConfig =
        attributeSchema->GetPackAttributeConfig("product_price");
    ASSERT_EQ(attrid_t(1), packAttrConfig->GetPackAttrId());
    compType = packAttrConfig->GetCompressType();
    ASSERT_EQ(string("product_price"), packAttrConfig->GetAttrName());
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
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "pack_attribute/"
                            + "pack_attribute_schema_with_error1.json"),
                 SchemaException);
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "pack_attribute/"
                            + "pack_attribute_schema_with_error2.json"),
                 SchemaException);
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "pack_attribute/"
                            + "pack_attribute_schema_with_error3.json"),
                 SchemaException);
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "pack_attribute/"
                            + "pack_attribute_schema_with_error4.json"),
                 SchemaException);
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "pack_attribute/"
                            + "main_sub_schema_with_duplicate_pack_attr.json"),
                 SchemaException);
}

void IndexPartitionSchemaTest::TestCaseForTTL()
{
    // "time_to_live" : 20
    IndexPartitionSchemaPtr schema =
        ReadSchema(string(TEST_DATA_PATH) + "ttl_test/kv_schema_default.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(20L, schema->GetDefaultTTL());
    string jsonString = ToJsonString(*schema);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(20L, cloneSchema->GetDefaultTTL());

    // "enable_ttl": true
    schema = ReadSchema(string(TEST_DATA_PATH) + "ttl_test/kv_schema_enable.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // "time_to_live" : 30, "enable_ttl": true
    schema = ReadSchema(string(TEST_DATA_PATH) + "ttl_test/kv_schema_both_1.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(30L, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(30L, cloneSchema->GetDefaultTTL());

    // "time_to_live" : 20, "enable_ttl": false
    schema = ReadSchema(string(TEST_DATA_PATH) + "ttl_test/kv_schema_both_2.json");
    ASSERT_FALSE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_FALSE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // "time_to_live" : -1, "enable_ttl": true
    schema = ReadSchema(string(TEST_DATA_PATH) + "ttl_test/kv_schema_both_3.json");
    ASSERT_TRUE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_TRUE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());

    // no ttl
    schema = ReadSchema(string(TEST_DATA_PATH) + "ttl_test/kv_schema_nottl.json");
    ASSERT_FALSE(schema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, schema->GetDefaultTTL());
    jsonString = ToJsonString(*schema);
    cloneSchema.reset(new IndexPartitionSchema("noname"));
    FromJsonString(*cloneSchema, jsonString);
    ASSERT_FALSE(cloneSchema->TTLEnabled());
    ASSERT_EQ(DEFAULT_TIME_TO_LIVE, cloneSchema->GetDefaultTTL());
}

void IndexPartitionSchemaTest::TestCaseForEnableTTL()
{
    {
        // 1. normal case
        IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH)
                + "ttl_test/schema1.json");
        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        ASSERT_TRUE(attrSchema != NULL);
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
        ASSERT_TRUE(attrConfig != NULL);
        FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
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
        // test ttl_field_name
        IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH)
                + "ttl_test/schema3.json");
        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        ASSERT_TRUE(attrSchema != NULL);
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
        ASSERT_TRUE(attrConfig == NULL);
        attrConfig = attrSchema->GetAttributeConfig("ttl_field");
        ASSERT_TRUE(attrConfig != NULL);
        FieldConfigPtr fieldConfig = schema->GetFieldSchema()->GetFieldConfig("ttl_field");
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
        IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH)
                + "ttl_test/schema2.json");
        AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
        ASSERT_TRUE(attrSchema != NULL);
        AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(DOC_TIME_TO_LIVE_IN_SECONDS);
        ASSERT_TRUE(attrConfig != NULL);
        ASSERT_EQ(fieldid_t(1), attrConfig->GetFieldConfig()->GetFieldId());
    }
    {
        bool hasException = false;
        try
        {
            IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH)
                    + "ttl_test/invalid_schema.json");
        }
        catch (const SchemaException &e)
        {
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }
}

void IndexPartitionSchemaTest::CheckVirtualAttribute(
        const AttributeSchemaPtr& virtualAttrSchema,
        const string& fieldName, fieldid_t fieldId)
{
    assert(virtualAttrSchema);
    AttributeConfigPtr virtualAttrConfig =
        virtualAttrSchema->GetAttributeConfig(fieldName);
    ASSERT_TRUE(virtualAttrConfig);
    ASSERT_EQ(fieldId, virtualAttrConfig->GetFieldId());
}

void IndexPartitionSchemaTest::TestCaseForKVIndexException()
{
    // unsupported compress type
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kv_index_exception.json"), BadParameterException);
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kv_index_exception1.json"), SchemaException);

    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kv_index_exception2.json"), SchemaException);
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kv_index_with_no_attribute.json"), SchemaException);
    // lack of fields
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kv_schema/kv_without_fields.json"), SchemaException);
    // fields in region schema
    ReadSchema(string(TEST_DATA_PATH) + "kv_schema/kv_with_fields_in_region_schema.json");
}

void IndexPartitionSchemaTest::TestCaseForKVIndex()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH) + "kv_index.json");

    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    cout << jsonString << endl;
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
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
    ASSERT_TRUE(kvIndexConfig);

    PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(attrConfig);
    vector<string> attrNames;
    attrConfig->GetSubAttributeNames(attrNames);
    ASSERT_THAT(attrNames, ElementsAre(
                    string("nid"), string("pidvid"), string("timestamp")));

    const KVIndexPreference& indexPreference = kvIndexConfig->GetIndexPreference();
    ASSERT_EQ(ipt_perf, indexPreference.GetType());
    const KVIndexPreference::ValueParam& valueParam = indexPreference.GetValueParam();
    ASSERT_FALSE(valueParam.IsEncode());
    ASSERT_EQ(string(""), valueParam.GetFileCompressType());
}

void IndexPartitionSchemaTest::TestCaseForKKVIndexException()
{
    // pkey set limit_count
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kkv_index_exception1.json"), SchemaException);
    
    // skey trunc_sort_param set no_exist field
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kkv_index_exception2.json"), SchemaException);

    // skey trunc_sort_param use multi value field
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kkv_index_exception3.json"), SchemaException);

    // unsupported compress type
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kkv_index_exception4.json"), BadParameterException);

    // key use multi value field
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kkv_index_exception5.json"), SchemaException);

    // skey use multi value field
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) + "kkv_index_exception6.json"), SchemaException);

}

void IndexPartitionSchemaTest::TestCaseForKKVIndex()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH) + "kkv_index.json");

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
    KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexConfig);
    ASSERT_TRUE(kkvIndexConfig);

    ASSERT_TRUE(kkvIndexConfig->NeedSuffixKeyTruncate());
    ASSERT_EQ((uint32_t)5000, kkvIndexConfig->GetSuffixKeyTruncateLimits());

    const SortParams& truncSortParam = kkvIndexConfig->GetSuffixKeyTruncateParams();
    ASSERT_EQ((size_t)1, truncSortParam.size());
    ASSERT_EQ("$TIME_STAMP", truncSortParam[0].GetSortField());
    ASSERT_TRUE(kkvIndexConfig->OptimizedStoreSKey());
                
    PackAttributeConfigPtr valueConfig =
        kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
    ASSERT_TRUE(valueConfig);
    vector<string> attrNames;
    valueConfig->GetSubAttributeNames(attrNames);
    ASSERT_THAT(attrNames, ElementsAre(
                    string("nick"), string("pidvid"), string("timestamp")));

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
    IndexPartitionSchemaPtr schema = ReadSchema(string(TEST_DATA_PATH)
            + "index_engine_example.json");
    auto &jsonMap = schema->GetUserDefinedParam();
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
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
            + "kv_index_with_region_field_schema.json");
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

        FieldSchemaPtr fieldSchema = regionSchema->GetFieldSchema();
        ASSERT_EQ(ft_int64, fieldSchema->GetFieldConfig("nid")->GetFieldType());
        ASSERT_TRUE(fieldSchema->GetFieldConfig("pidvid")->IsMultiValue());
        
        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, ElementsAre(
                        string("nid"), string("pidvid"), string("timestamp")));

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
        
        FieldSchemaPtr fieldSchema = regionSchema->GetFieldSchema();
        ASSERT_EQ(ft_int32, fieldSchema->GetFieldConfig("nid")->GetFieldType());
        ASSERT_FALSE(fieldSchema->GetFieldConfig("pidvid")->IsMultiValue());
        
        IndexSchemaPtr indexSchema = regionSchema->GetIndexSchema();
        ASSERT_EQ(it_kv, indexSchema->GetPrimaryKeyIndexType());
        SingleFieldIndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, ElementsAre(
                        string("nid"), string("pidvid")));

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
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
            + "schema_with_modify_operations_2.json");
    Any any = ToJson(*loadSchema);
    string jsonString = ToString(any);
    Any comparedAny = ParseJson(jsonString);
    IndexPartitionSchemaPtr cloneSchema(new IndexPartitionSchema("noname"));
    FromJson(*cloneSchema, comparedAny);
    IndexPartitionSchemaPtr schema(cloneSchema->Clone());
    schema->Check();
    schema->AssertEqual(*loadSchema);

    CheckFieldSchema(schema->GetFieldSchema(),
                     "string1:normal;string2:normal;price:normal;"
                     "nid:delete;new_nid:normal;nid:normal");
    
    CheckIndexSchema(schema->GetIndexSchema(),
                     "index2:delete;pk:normal;nid:delete;new_nid:normal;nid:normal");
    
    CheckAttributeSchema(schema->GetAttributeSchema(),
                         "string1:normal;string2:delete;price:delete;"
                         "nid:delete;new_nid:normal;nid:normal");

    ASSERT_TRUE(schema->HasModifyOperations());
    ASSERT_EQ(3, schema->GetModifyOperationCount());
    size_t i = 1;
    for (; i <= schema->GetModifyOperationCount(); i++)
    {
        auto op = schema->GetSchemaModifyOperation(i);
        ASSERT_TRUE(op);
        ASSERT_EQ((schema_opid_t)i, op->GetOpId());
        ASSERT_TRUE(!op->GetParams().empty());
    }
    ASSERT_FALSE(schema->GetSchemaModifyOperation(i));
}

void IndexPartitionSchemaTest::TestDeleteMultiTimes()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
                                                    + "schema_with_modify_operations_2.json");
    {
        // delete index & attribute, add them again
        IndexConfigIteratorPtr indexConfigs =
            loadSchema->GetIndexSchema()->CreateIterator(true, CIT_DELETE);
        vector<string> deletedIndexName;
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++)
        {
            deletedIndexName.push_back((*iter)->GetIndexName());
        }
        ASSERT_THAT(deletedIndexName, ElementsAre("index2", "nid"));

        AttributeConfigIteratorPtr attrConfigs =
            loadSchema->GetAttributeSchema()->CreateIterator(CIT_DELETE);
        vector<string> deletedAttrName;
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
        {
            deletedAttrName.push_back((*iter)->GetAttrName());
        }
        ASSERT_THAT(deletedAttrName, ElementsAre("string2", "price", "nid"));
    }
    {
        // make add attribute not ready
        loadSchema->MarkOngoingModifyOperation(3);
        AttributeConfigIteratorPtr attrConfigs =
            loadSchema->GetAttributeSchema()->CreateIterator(CIT_DISABLE);
        vector<string> disableAttrName;
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
        {
            disableAttrName.push_back((*iter)->GetAttrName());
        }
        ASSERT_THAT(disableAttrName, ElementsAre("nid"));
        attrConfigs =
            loadSchema->GetAttributeSchema()->CreateIterator(CIT_DELETE);
        vector<string> deletedAttrName;
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
        {
            deletedAttrName.push_back((*iter)->GetAttrName());
        }
        ASSERT_THAT(deletedAttrName, ElementsAre("string2", "price", "nid"));
        // make add index not ready
        IndexConfigIteratorPtr indexConfigs =
            loadSchema->GetIndexSchema()->CreateIterator(true, CIT_DELETE);
        vector<string> deletedIndexName;
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++)
        {
            deletedIndexName.push_back((*iter)->GetIndexName());
        }
        ASSERT_THAT(deletedIndexName, ElementsAre("index2", "nid"));
        indexConfigs =
            loadSchema->GetIndexSchema()->CreateIterator(true, CIT_DISABLE);
        vector<string> disableIndexName;
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++)
        {
            disableIndexName.push_back((*iter)->GetIndexName());
        }
        ASSERT_THAT(disableIndexName, ElementsAre("nid"));
    }
}

void IndexPartitionSchemaTest::TestSupportAutoUpdate()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
                                                    + "support_auto_update_schema.json");
    AttributeConfigIteratorPtr attrConfigs =
        loadSchema->GetAttributeSchema()->CreateIterator(CIT_DELETE);
    vector<string> deletedAttrName;
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
    {
        deletedAttrName.push_back((*iter)->GetAttrName());
    }
    ASSERT_THAT(deletedAttrName, ElementsAre("string2", "price", "nid"));
    // disable not updatable attribute 
    loadSchema->MarkOngoingModifyOperation(3);
    ASSERT_TRUE(loadSchema->GetAttributeSchema()->GetAttributeConfig("nid")->IsDisable());
    ASSERT_TRUE(loadSchema->SupportAutoUpdate());
}

void IndexPartitionSchemaTest::TestCaseForMarkOngoingOperation()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
            + "schema_with_modify_operations.json");
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
    ASSERT_TRUE(schema->GetIndexSchema()->GetIndexConfig("nid")->IsDisable());
    ASSERT_TRUE(schema->GetAttributeSchema()->GetAttributeConfig("nid")->IsDisable());

    vector<schema_opid_t> notReadyIds;
    schema->GetNotReadyModifyOperationIds(notReadyIds);
    ASSERT_EQ(1, notReadyIds.size());
    ASSERT_EQ(3, notReadyIds[0]);
}

void IndexPartitionSchemaTest::TestCaseForInvalidModifyOperation()
{
    // test delete field nid still in use by index
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) +
                            "invalid_schema1_with_modify_operations.json"), SchemaException);

    // test delete no_exist index
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) +
                            "invalid_schema2_with_modify_operations.json"), SchemaException);

    // test deplicated add nid index
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) +
                            "invalid_schema3_with_modify_operations.json"), SchemaException);

    // test user set schemaid
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) +
                            "invalid_schema4_with_modify_operations.json"), SchemaException);

    // test empty modify operation
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) +
                            "invalid_schema5_with_modify_operations.json"), SchemaException);

    // test add truncate index
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) +
                            "add_truncate_index_with_modify_operations.json"), UnSupportedException);
    // test add adaptive bitmap index
    ASSERT_THROW(ReadSchema(string(TEST_DATA_PATH) +
                            "add_adaptive_bitmap_index_with_modify_operations.json"), UnSupportedException);
}

void IndexPartitionSchemaTest::TestCaseForAddNonVirtualException()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
            + "schema_with_modify_operations.json");
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
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
            + "schema_with_modify_operations.json");
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

void IndexPartitionSchemaTest::CheckFieldSchema(
        const FieldSchemaPtr& schema, const string& infoStr)
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
        if (status == "delete")
        {
            ASSERT_TRUE(!schema->GetFieldConfig(i));
            ASSERT_TRUE(config->IsDeleted());
        }
        else if (status == "normal")
        {
            FieldConfigPtr tmp = schema->GetFieldConfig(fieldName);
            ASSERT_EQ(i, tmp->GetFieldId());
            tmp = schema->GetFieldConfig(i);
            ASSERT_EQ(fieldName, tmp->GetFieldName());
            ASSERT_TRUE(config->IsNormal());            
        }
        else
        {
            assert(false);
        }
        ++iter;
    }
    ASSERT_EQ(iter, schema->End());

    FieldConfigIteratorPtr configIter = schema->CreateIterator(CIT_NORMAL);
    iter = configIter->Begin();
    for (fieldid_t i = 0; i < (fieldid_t)fieldInfos.size(); i++) {
        const string& fieldName = fieldInfos[i][0];
        const string& status = fieldInfos[i][1];
        if (status != "normal")
        {
            continue;
        }
        FieldConfigPtr config = *iter;
        ASSERT_EQ(i, config->GetFieldId());
        ASSERT_EQ(fieldName, config->GetFieldName());
        ++iter;
    }
    ASSERT_EQ(iter, configIter->End());    
}

void IndexPartitionSchemaTest::CheckIndexSchema(
        const IndexSchemaPtr& schema, const string& infoStr)
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

        SingleFieldIndexConfigPtr singleFieldIndexConf =
            DYNAMIC_POINTER_CAST(SingleFieldIndexConfig, config);
        fieldid_t fieldId = singleFieldIndexConf->GetFieldConfig()->GetFieldId();
        assert(fieldId != INVALID_FIELDID);
        
        ASSERT_EQ(i, config->GetIndexId());
        ASSERT_EQ(indexName, config->GetIndexName());
        if (status == "delete")
        {
            ASSERT_TRUE(!schema->GetIndexConfig(i));
            ASSERT_TRUE(config->IsDeleted());
            ASSERT_TRUE(schema->GetIndexIdList(fieldId).empty());
            ASSERT_FALSE(schema->IsInIndex(fieldId));
        }
        else if (status == "normal")
        {
            IndexConfigPtr tmp = schema->GetIndexConfig(indexName);
            ASSERT_EQ(i, tmp->GetIndexId());
            tmp = schema->GetIndexConfig(i);
            ASSERT_EQ(indexName, tmp->GetIndexName());
            ASSERT_TRUE(config->IsNormal());
            ASSERT_EQ(i, schema->GetIndexIdList(fieldId)[0]);
            ASSERT_TRUE(schema->IsInIndex(fieldId));
        }
        else
        {
            assert(false);
        }
        ++iter;
    }
    ASSERT_EQ(iter, schema->End());

    IndexConfigIteratorPtr configIter = schema->CreateIterator(CIT_NORMAL);
    iter = configIter->Begin();
    for (indexid_t i = 0; i < (indexid_t)indexInfos.size(); i++) {
        const string& indexName = indexInfos[i][0];
        const string& status = indexInfos[i][1];
        if (status != "normal")
        {
            continue;
        }
        IndexConfigPtr config = *iter;
        ASSERT_EQ(i, config->GetIndexId());
        ASSERT_EQ(indexName, config->GetIndexName());
        ++iter;
    }
    ASSERT_EQ(iter, configIter->End());    
}

void IndexPartitionSchemaTest::CheckAttributeSchema(
        const AttributeSchemaPtr& schema, const string& infoStr)
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
        if (status == "delete")
        {
            ASSERT_TRUE(!schema->GetAttributeConfig(i));
            ASSERT_TRUE(config->IsDeleted());
            ASSERT_FALSE(schema->IsInAttribute(fieldId));
            ASSERT_FALSE(schema->GetAttributeConfigByFieldId(fieldId));
        }
        else if (status == "normal")
        {
            AttributeConfigPtr tmp = schema->GetAttributeConfig(attributeName);
            ASSERT_EQ(i, tmp->GetAttrId());
            tmp = schema->GetAttributeConfig(i);
            ASSERT_EQ(attributeName, tmp->GetAttrName());
            ASSERT_TRUE(config->IsNormal());
            ASSERT_TRUE(schema->IsInAttribute(fieldId));
        }
        else
        {
            assert(false);
        }
        ++iter;
    }
    ASSERT_EQ(iter, schema->End());

    AttributeConfigIteratorPtr configIter = schema->CreateIterator(CIT_NORMAL);
    iter = configIter->Begin();
    for (attrid_t i = 0; i < (attrid_t)attributeInfos.size(); i++) {
        const string& attributeName = attributeInfos[i][0];
        const string& status = attributeInfos[i][1];
        if (status != "normal")
        {
            continue;
        }
        AttributeConfigPtr config = *iter;
        ASSERT_EQ(i, config->GetAttrId());
        ASSERT_EQ(attributeName, config->GetAttrName());
        ++iter;
    }
    ASSERT_EQ(iter, configIter->End());    
}

void IndexPartitionSchemaTest::TestCaseForMultiRegionKKVSchema()
{
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
            + "kkv_index_multi_region.json");
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
        KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexConfig);
        ASSERT_TRUE(kkvIndexConfig);

        PackAttributeConfigPtr attrConfig = kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, ElementsAre(
                        string("nick"), string("pidvid")));

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
        KKVIndexConfigPtr kkvIndexConfig = DYNAMIC_POINTER_CAST(KKVIndexConfig, indexConfig);
        ASSERT_TRUE(kkvIndexConfig);

        PackAttributeConfigPtr attrConfig =
            kkvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, ElementsAre(
                        string("nick"), string("nid"), string("timestamp")));

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
    IndexPartitionSchemaPtr loadSchema = ReadSchema(string(TEST_DATA_PATH)
            + "kv_index_multi_region.json");
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
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, ElementsAre(
                        string("nid"), string("pidvid"), string("timestamp")));

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
        KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexConfig);
        ASSERT_TRUE(kvIndexConfig);

        PackAttributeConfigPtr attrConfig = kvIndexConfig->GetValueConfig()->CreatePackAttributeConfig();
        ASSERT_TRUE(attrConfig);
        vector<string> attrNames;
        attrConfig->GetSubAttributeNames(attrNames);
        ASSERT_THAT(attrNames, ElementsAre(
                        string("nid"), string("pidvid")));

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
    ASSERT_ANY_THROW(ReadSchema(string(TEST_DATA_PATH) + "schema_no_indexs_exception.json"));
}

void IndexPartitionSchemaTest::TestCaseForCustomizedTable()
{
    IndexPartitionSchemaPtr schema = ReadSchema(
        string(TEST_DATA_PATH) + "schema/" + 
        "customized_table_schema.json");
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
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + 
            "schema/normal_table_with_customized_schema.json"));
    }
    {
        // customized table without attributes and indexs
        ASSERT_NO_THROW(ReadSchema(
            string(TEST_DATA_PATH) +
            "schema/customized_table_without_attributes_indexs_schema.json"));
    }
    {
        // case 2: customized but without customized parts
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) +
            "schema/customized_table_without_customized_schema.json"));
    }
    {
        // case 3: customized document only
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/" + 
            "customized_document_only_schema.json"));         
    }
    {
        // case 4: customized table miss plugin name
        // without plugin name field 
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/" + 
            "customized_table_miss_plugin_field_schema.json")); 
        // with plugin name field 
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/" + 
            "customized_table_miss_plugin_content_schema.json")); 
    }
    {
        // case 5: customized document miss id
        // without id field
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/" + 
            "customized_document_miss_id_field_schema.json")); 
        // with id field
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/" + 
            "customized_document_miss_id_content_schema.json")); 
    }
    {
        // case 6: error parameters
        ASSERT_ANY_THROW(ReadSchema(string(TEST_DATA_PATH) +
                                    "schema/customized_table_with_error_parameters_schema.json"));
        ASSERT_ANY_THROW(ReadSchema(string(TEST_DATA_PATH) +
                                    "schema/customized_document_with_error_parameters_schema.json")); 
    }
    
    // field  level
    {
        // lack of id or empty id
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema1.json"));
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema2.json")); 
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema3.json")); 
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema4.json"));         

        // lack of pluginName or empty pluginName
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema5.json"));
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema6.json"));
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema7.json"));
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema8.json"));

        // error parameters
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_field_schema9.json"));

        // kv/kkv table with customized accessor or format
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_kv_schema1.json"));
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_kv_schema2.json"));
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_kkv_schema1.json"));
        ASSERT_ANY_THROW(ReadSchema(
            string(TEST_DATA_PATH) + "schema/customized_kkv_schema2.json"));
    }
}

void IndexPartitionSchemaTest::TestCaseForAddSpatialIndex()
{
    IndexPartitionSchemaPtr oldSchema = SchemaMaker::MakeSchema(
            "field1:long;field2:uint32",
            "numberIndex:number:field1;", "", "");

    IndexPartitionSchemaPtr newSchema(oldSchema->Clone());

    // only add spatial index
    ModifySchemaMaker::AddModifyOperations(newSchema,
            "", "coordinate:location", "spatial_index:spatial:coordinate", "");

    IndexConfigPtr indexConfig =
        newSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_TRUE(indexConfig);
    ASSERT_EQ(1, indexConfig->GetOwnerModifyOperationId());

    AttributeConfigPtr attrConfig =
        newSchema->GetAttributeSchema()->GetAttributeConfig("coordinate");
    ASSERT_TRUE(attrConfig);
    ASSERT_EQ(1, attrConfig->GetOwnerModifyOperationId());
    ASSERT_EQ(AttributeConfig::ct_index_accompany, attrConfig->GetConfigType());

    ModifySchemaMaker::AddModifyOperations(newSchema, "indexs=spatial_index", "", "", "");
    indexConfig = newSchema->GetIndexSchema()->GetIndexConfig("spatial_index");
    ASSERT_FALSE(indexConfig);
    attrConfig = newSchema->GetAttributeSchema()->GetAttributeConfig("coordinate");
    ASSERT_FALSE(attrConfig);

    // add spatial index & attribute
    ModifySchemaMaker::AddModifyOperations(newSchema,
            "", "", "spatial_index:spatial:coordinate", "coordinate");
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
    IndexPartitionSchemaPtr schema = ReadSchema(
            string(TEST_DATA_PATH) + "truncate_example_with_shard.json");
    ModifySchemaMaker::AddModifyOperations(schema, "indexs=phrase", "", "", "");
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    IndexConfigIteratorPtr iter = indexSchema->CreateIterator(true, CIT_DELETE);
    size_t count = 0;
    for (auto it = iter->Begin(); it != iter->End(); it++)
    {
        cout << (*it)->GetIndexName() << endl;
        ++count;
    }
    // shard_num = 4, truncate_profile_num = 2
    // count = (4 + 1) * (2 + 1)
    ASSERT_EQ(15, count);
}

void IndexPartitionSchemaTest::TestDeleteAttribute()
{
    IndexPartitionSchemaPtr schema = ReadSchema(
            string(TEST_DATA_PATH) + "truncate_example_with_shard.json");
    
    ASSERT_THROW(ModifySchemaMaker::AddModifyOperations(schema, "attributes=user_id", "", "", ""),
                 SchemaException);
    ASSERT_THROW(ModifySchemaMaker::AddModifyOperations(schema, "attributes=price", "", "", ""),
                 SchemaException);
}

void IndexPartitionSchemaTest::TestCreateIterator()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "field1:long;field2:uint32",
            "numberIndex:number:field1;", "field1;field2", "");

    // add spatial_index & cooridnate as accompany
    ModifySchemaMaker::AddModifyOperations(schema,
            "", "coordinate:location", "spatial_index:spatial:coordinate", "");

    CheckIterator(schema, "field", CIT_NORMAL, {"field1", "field2", "coordinate"});    
    CheckIterator(schema, "index", CIT_NORMAL, {"numberIndex", "spatial_index"});
    CheckIterator(schema, "attribute", CIT_NORMAL, {"field1", "field2", "coordinate"});

    // delete spatial_index & coordinate as accompany, field1
    ModifySchemaMaker::AddModifyOperations(schema, "attributes=field1;indexs=spatial_index", "", "", "");
    CheckIterator(schema, "field", CIT_NORMAL, {"field1", "field2", "coordinate"});    
    CheckIterator(schema, "index", CIT_NORMAL, {"numberIndex"});
    CheckIterator(schema, "attribute", CIT_NORMAL, {"field2"});
    CheckIterator(schema, "index", CIT_DELETE, {"spatial_index"});
    CheckIterator(schema, "attribute", CIT_DELETE, {"field1", "coordinate"});

    // delete field2
    // add spatial_index & coordinate & field3
    ModifySchemaMaker::AddModifyOperations(schema,
            "fields=field2;attributes=field2", "field3:string",
            "spatial_index:spatial:coordinate", "coordinate;field3");
    schema->MarkOngoingModifyOperation(3);

    CheckIterator(schema, "field", CIT_NORMAL, {"field1", "coordinate", "field3"});
    CheckIterator(schema, "field", CIT_ALL, {"field1", "field2", "coordinate", "field3"});
    CheckIterator(schema, "field", CIT_DELETE, {"field2"});

    CheckIterator(schema, "index", CIT_ALL, {"numberIndex", "spatial_index", "spatial_index"});
    CheckIterator(schema, "index", CIT_NORMAL, {"numberIndex"});    
    CheckIterator(schema, "index", CIT_DISABLE, {"spatial_index"});
    CheckIterator(schema, "index", CIT_DELETE, {"spatial_index"});

    CheckIterator(schema, "attribute", CIT_ALL, {"field1", "field2", "coordinate", "coordinate", "field3"});
    CheckIterator(schema, "attribute", CIT_NORMAL, {});    
    CheckIterator(schema, "attribute", CIT_DISABLE, {"coordinate", "field3"});
    CheckIterator(schema, "attribute", CIT_DELETE, {"field1", "field2", "coordinate"});
}

void IndexPartitionSchemaTest::TestEffectiveIndexInfo()
{
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(
            "field1:long;field2:uint32",
            "numberIndex:number:field1;", "field1;field2", "");

    // add spatial_index & cooridnate as accompany
    ModifySchemaMaker::AddModifyOperations(schema,
            "", "coordinate:location", "spatial_index:spatial:coordinate", "");
    CheckEffectiveIndexInfo(schema, 1, {},
                            "numberIndex:NUMBER;spatial_index:SPATIAL",
                            "field1:LONG;field2:UINT32;coordinate:LOCATION");

    CheckEffectiveIndexInfo(schema, 1, {},
                            "spatial_index:SPATIAL", "coordinate:LOCATION", true);
    
    // delete spatial_index & coordinate as accompany, field1
    ModifySchemaMaker::AddModifyOperations(schema,
            "attributes=field1;indexs=spatial_index", "", "", "");
    CheckEffectiveIndexInfo(schema, 2, {}, "numberIndex:NUMBER", "field2:UINT32");

    // delete field2
    // add spatial_index & coordinate & field3
    ModifySchemaMaker::AddModifyOperations(schema,
            "fields=field2;attributes=field2", "field3:string",
            "spatial_index:spatial:coordinate", "coordinate;field3");
    CheckEffectiveIndexInfo(schema, 3, {3}, "numberIndex:NUMBER", "");
}

void IndexPartitionSchemaTest::CheckEffectiveIndexInfo(
        const IndexPartitionSchemaPtr& schema, schema_opid_t opId,
        const vector<schema_opid_t>& ongoingId,        
        const string& expectIndexs, const string& expectAttrs, bool onlyModifyItem)
{
    IndexPartitionSchemaPtr targetSchema(
            schema->CreateSchemaForTargetModifyOperation(opId));
    for (const auto& id : ongoingId)
    {
        targetSchema->MarkOngoingModifyOperation(id);
    }
    Any any = ParseJson(targetSchema->GetEffectiveIndexInfo(onlyModifyItem));
    JsonMap jsonMap = AnyCast<JsonMap>(any);
    auto parseItem = [&jsonMap](const string& key)
    {
        vector<ModifyItemPtr> items;
        JsonMap::iterator iter = jsonMap.find(key);
        if (iter != jsonMap.end())
        {
            FromJson(items, iter->second);
        }
        return items;
    };
                         
    vector<ModifyItemPtr> indexItems = parseItem(INDEXS);
    vector<ModifyItemPtr> attrItems = parseItem(ATTRIBUTES);
    auto checkItems = [](const string& expectInfosStr, const vector<ModifyItemPtr>& items)
    {
        vector<vector<string>> expectIndexInfos;
        StringUtil::fromString(expectInfosStr, expectIndexInfos, ":", ";");
        ASSERT_EQ(items.size(), expectIndexInfos.size());
        for (size_t i = 0; i < items.size(); i++)
        {
            assert(expectIndexInfos[i].size() == 2);
            ASSERT_EQ(items[i]->GetName(), expectIndexInfos[i][0]);
            ASSERT_EQ(items[i]->GetType(), expectIndexInfos[i][1]);        
        }
    };
    checkItems(expectIndexs, indexItems);
    checkItems(expectAttrs, attrItems);    
}

void IndexPartitionSchemaTest::CheckIterator(const IndexPartitionSchemaPtr& schema,
        const string& target, ConfigIteratorType type, const vector<string>& expectNames)
{
#define CHECK_ITERATOR(funcName, iter, expectNames)             \
    size_t idx = 0;                                             \
    for (auto it = iter->Begin(); it != iter->End(); it++)      \
    {                                                           \
        ASSERT_TRUE(idx < expectNames.size());                  \
        ASSERT_EQ(expectNames[idx++], (*it)->funcName());       \
    }                                                           \
    ASSERT_EQ(expectNames.size(), idx);                         \
    
    if (target == "index")
    {
        auto iter = schema->GetIndexSchema()->CreateIterator(false, type);
        CHECK_ITERATOR(GetIndexName, iter, expectNames);
    }
    else if (target == "attribute")
    {
        auto iter = schema->GetAttributeSchema()->CreateIterator(type);
        CHECK_ITERATOR(GetAttrName, iter, expectNames);        
    }
    else if (target == "field")
    {
        auto iter = schema->GetFieldSchema()->CreateIterator(type);
        CHECK_ITERATOR(GetFieldName, iter, expectNames);        
    }
    else
    {
        assert(false);
    }

#undef CHECK_ITERATOR
}

void IndexPartitionSchemaTest::TestCaseForModifyOperationWithTruncateIndex()
{
    string confFile = string(TEST_DATA_PATH) + "modify_operation_with_truncate_index.json";
    string jsonString;
    FileSystemWrapper::AtomicLoad(confFile, jsonString);

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

IE_NAMESPACE_END(config);

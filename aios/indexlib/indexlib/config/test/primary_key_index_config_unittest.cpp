#include "indexlib/config/test/primary_key_index_config_unittest.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PrimaryKeyIndexConfigTest);

PrimaryKeyIndexConfigTest::PrimaryKeyIndexConfigTest()
{
}

PrimaryKeyIndexConfigTest::~PrimaryKeyIndexConfigTest()
{
}

void PrimaryKeyIndexConfigTest::CaseSetUp()
{
}

void PrimaryKeyIndexConfigTest::CaseTearDown()
{
}

void PrimaryKeyIndexConfigTest::TestCheckPrimaryKey()
{
    {
        //test normal case
        PrimaryKeyIndexConfig testConfig("pk", it_primarykey64);
        FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_int32, false));
        testConfig.SetFieldConfig(fieldConfig);
        testConfig.SetPrimaryKeyHashType(pk_number_hash);
        ASSERT_NO_THROW(testConfig.Check());
    }

    {
        //test not pk
        PrimaryKeyIndexConfig testConfig("pk", it_number);
        FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_int32, false));
        testConfig.SetFieldConfig(fieldConfig);
        testConfig.SetPrimaryKeyHashType(pk_number_hash);
        ASSERT_THROW(testConfig.Check(), SchemaException);
    }

    {
        //test multi value
        PrimaryKeyIndexConfig testConfig("pk", it_primarykey64);
        FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_int32, true));
        testConfig.SetFieldConfig(fieldConfig);
        testConfig.SetPrimaryKeyHashType(pk_number_hash);
        ASSERT_THROW(testConfig.Check(), SchemaException);
    }

    {
        //test field type not number
        PrimaryKeyIndexConfig testConfig("pk", it_primarykey64);
        FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_string, false));
        testConfig.SetFieldConfig(fieldConfig);
        testConfig.SetPrimaryKeyHashType(pk_number_hash);
        ASSERT_THROW(testConfig.Check(), SchemaException);
    }
}

void PrimaryKeyIndexConfigTest::TestJsonizePkIndexType()
{
    CheckJsonizePkIndexType("{}", pk_sort_array, false);
    CheckJsonizePkIndexType("{\"pk_storage_type\" : \"hash_table\" }", pk_hash_table, false);
    CheckJsonizePkIndexType("{\"pk_storage_type\" : \"sort_array\"}", pk_sort_array, false);
    CheckJsonizePkIndexType("{\"pk_storage_type\" : \"invalid_option\"}", pk_sort_array, true);
}

void PrimaryKeyIndexConfigTest::CheckJsonizePkIndexType(
    const std::string& jsonStr, PrimaryKeyIndexType expectType,
    bool isExpectException)
{
    PrimaryKeyIndexConfig pkIndexConfig("pk", it_primarykey64);
    if (isExpectException)
    {
        ASSERT_THROW(FromJsonString(pkIndexConfig, jsonStr), SchemaException);
    }
    else
    {
        ASSERT_NO_THROW(FromJsonString(pkIndexConfig, jsonStr));
        ASSERT_EQ(expectType, pkIndexConfig.GetPrimaryKeyIndexType());
        string copyStr = ToJsonString(pkIndexConfig);
        PrimaryKeyIndexConfig newPkIndexConfig("pk", it_primarykey64);
        FromJsonString(newPkIndexConfig, copyStr);
        ASSERT_EQ(expectType, newPkIndexConfig.GetPrimaryKeyIndexType());
    }
}


IE_NAMESPACE_END(config);


#ifndef __INDEXLIB_PRIMARY_KEY_TEST_CASE_HELPER_H
#define __INDEXLIB_PRIMARY_KEY_TEST_CASE_HELPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/test/partition_schema_maker.h"

IE_NAMESPACE_BEGIN(index);

class PrimaryKeyTestCaseHelper
{
public:
    PrimaryKeyTestCaseHelper();
    ~PrimaryKeyTestCaseHelper();
public:
    template<typename Key>
    static config::IndexConfigPtr CreateIndexConfig(const std::string& indexName,
            PrimaryKeyIndexType pkIndexType, bool hasPKAttribute = true);

    template<typename Key>
    static config::IndexPartitionSchemaPtr CreatePrimaryKeySchema(bool hasPKAttribute = true);

    static void MakeFakePrimaryKeyDocStr(uint32_t urls[], 
            uint32_t urlCount, std::string& docStr);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyTestCaseHelper);

template<typename Key>
config::IndexConfigPtr 
PrimaryKeyTestCaseHelper::CreateIndexConfig(const std::string& indexName,
        PrimaryKeyIndexType pkIndexType, bool hasPKAttribute)
{
    IndexType indexType;
    if (typeid(Key) == typeid(uint64_t))
    {
        indexType = it_primarykey64;
    }
    else
    {
        indexType = it_primarykey128;
    }
        
    config::PrimaryKeyIndexConfig* singleConfig = 
        new config::PrimaryKeyIndexConfig(indexName, indexType);
    config::FieldConfigPtr fieldConfig(new config::FieldConfig(indexName, ft_string, false));
    singleConfig->SetFieldConfig(fieldConfig);
    singleConfig->SetPrimaryKeyAttributeFlag(hasPKAttribute);
    singleConfig->SetPrimaryKeyIndexType(pkIndexType);
    if (pkIndexType == pk_hash_table)
    {
        singleConfig->SetPrimaryKeyLoadParam(
            config::PrimaryKeyLoadStrategyParam::HASH_TABLE, false);
    }
    
    return config::IndexConfigPtr(singleConfig);
}

template<typename Key>
config::IndexPartitionSchemaPtr PrimaryKeyTestCaseHelper::CreatePrimaryKeySchema(
        bool hasPKAttribute)
{
    std::string pkField;
    if (typeid(Key) == typeid(uint64_t))
    {
        pkField = "pk:primarykey64:url";
    }
    else
    {
        pkField = "pk:primarykey128:url";
    }
    config::IndexPartitionSchemaPtr schema;
    schema.reset(new config::IndexPartitionSchema);
    PartitionSchemaMaker::MakeSchema(schema,
            "url:string;", // above is field schema
            pkField.c_str(), // above is index schema
            "", // above is attribute schema
            "" );// above is summary schema
    if (hasPKAttribute)
    {
        config::IndexSchemaPtr indexSchema = schema->GetIndexSchema();
        config::IndexConfigPtr indexConfig = indexSchema->GetPrimaryKeyIndexConfig();
        config::PrimaryKeyIndexConfigPtr singleFiledConfig = 
            DYNAMIC_POINTER_CAST(config::PrimaryKeyIndexConfig, indexConfig);
        singleFiledConfig->SetPrimaryKeyAttributeFlag(true);
    }
    return schema;
}

inline void PrimaryKeyTestCaseHelper::MakeFakePrimaryKeyDocStr(
        uint32_t urls[], uint32_t urlCount, std::string& docStr)
{
    docStr.clear();
    for (uint32_t i = 0; i < urlCount; ++i)
    {
        std::stringstream ss;
        ss << "{[url: (url" << urls[i] << ")]};";
        docStr.append(ss.str());
    }
}


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_TEST_CASE_HELPER_H

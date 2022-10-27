#include "indexlib/indexlib.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/impl/kv_index_config_impl.h"
#include "indexlib/config/number_index_type_transformor.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/misc/exception.h"
#include <algorithm>
#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);

IE_LOG_SETUP(config, SchemaConfigurator);

void SchemaConfigurator::Configurate(
    const Any& any,
    IndexPartitionSchemaImpl& schema,
    bool isSubSchema) 
{
    JsonMap indexPartitionMap = AnyCast<JsonMap>(any);
    
    //parse table name.
    JsonMap::iterator iter = indexPartitionMap.find(TABLE_NAME);
    if (iter == indexPartitionMap.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "no table_name defined");
    }
    string schemaName = AnyCast<string>(iter->second);
    if (schemaName.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "table_name must be specified and not empty");
    }
    schema.SetSchemaName(schemaName);

    //parse table type
    iter = indexPartitionMap.find(TABLE_TYPE);
    if (iter != indexPartitionMap.end())
    {
        schema.SetTableType(AnyCast<string>(iter->second));
    }
    
    //load customized config for table and document
    if (schema.GetTableType() == tt_customized)
    {
        LoadCustomizedConfig(any, schema);   
    }

    //parse user defined param
    iter = indexPartitionMap.find(TABLE_USER_DEFINED_PARAM);
    if (iter != indexPartitionMap.end())
    {
        schema.SetUserDefinedParam(AnyCast<JsonMap>(iter->second));
    }

    //parse global region index preference
    iter = indexPartitionMap.find(GLOBAL_REGION_INDEX_PREFERENCE);
    if (iter != indexPartitionMap.end())
    {
        schema.SetGlobalRegionIndexPreference(AnyCast<JsonMap>(iter->second));
    }
    
    //parse adaptive dictionaries definition
    iter = indexPartitionMap.find(ADAPTIVE_DICTIONARIES);
    if (iter != indexPartitionMap.end())
    {
        LoadAdaptiveDictSchema(indexPartitionMap, schema);
    }

    //parse dictionaries definition
    iter = indexPartitionMap.find(DICTIONARIES);
    if (iter != indexPartitionMap.end())
    {
        LoadDictSchema(iter->second, schema);
    }

    //parse fields definition
    iter = indexPartitionMap.find(FIELDS);
    if (iter != indexPartitionMap.end())
    {
        schema.LoadFieldSchema(iter->second);
    }
    LoadRegions(indexPartitionMap, schema);

    iter = indexPartitionMap.find(SUB_SCHEMA);
    if (iter != indexPartitionMap.end()) {
        if (isSubSchema) {
            INDEXLIB_FATAL_ERROR(Schema, "sub schema can not has sub schema!");
        }
        AddOfflineJoinVirtualAttribute(MAIN_DOCID_TO_SUB_DOCID_ATTR_NAME, schema);
    }
    if (isSubSchema) {
        AddOfflineJoinVirtualAttribute(SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, schema);
    }
  
    iter = indexPartitionMap.find(MAX_SCHEMA_MODIFY_OPERATION_COUNT);
    if (iter != indexPartitionMap.end())
    {
        uint32_t count = AnyCast<uint32_t>(iter->second);
        schema.SetMaxModifyOperationCount(count);
    }
    
    // parse modify_operations definition
    iter = indexPartitionMap.find(SCHEMA_MODIFY_OPERATIONS);
    if (iter != indexPartitionMap.end())
    {
        LoadModifyOperations(iter->second, schema);
    }

    //parse schema id
    iter = indexPartitionMap.find(SCHEMA_VERSION_ID);
    if (iter != indexPartitionMap.end())
    {
        if (schema.HasModifyOperations())
        {
            INDEXLIB_FATAL_ERROR(Schema, "not support set schemaId when has modify operations");
        }
        schema.SetSchemaVersionId(JsonNumberCast<schemavid_t>(iter->second));
    }

    // parse auto_update_preference
    iter = indexPartitionMap.find(AUTO_UPDATE_PREFERENCE);
    if (iter != indexPartitionMap.end())
    {
        bool autoUpdate = AnyCast<bool>(iter->second);
        schema.SetAutoUpdatePreference(autoUpdate);
    }
}

AdaptiveDictionarySchemaPtr SchemaConfigurator::LoadAdaptiveDictSchema(
        const Any& any, IndexPartitionSchemaImpl& schema)
{
    AdaptiveDictionarySchemaPtr adaptiveDictSchema(
            new AdaptiveDictionarySchema);

    Jsonizable::JsonWrapper jsonWrapper(any);
    adaptiveDictSchema->Jsonize(jsonWrapper);
    schema.SetAdaptiveDictSchema(adaptiveDictSchema);
    return adaptiveDictSchema;
}

DictionarySchemaPtr SchemaConfigurator::LoadDictSchema(const Any& any,
                                                       IndexPartitionSchemaImpl& schema)
{
    JsonArray dictionaries = AnyCast<JsonArray>(any);
    if (dictionaries.empty())
    {
        return DictionarySchemaPtr();
    }

    JsonArray::iterator iter = dictionaries.begin();
    for (; iter != dictionaries.end(); ++iter)
    {
        LoadDictionaryConfig(*iter, schema);
    }

    return schema.GetDictSchema();
}

DictionaryConfigPtr SchemaConfigurator::LoadDictionaryConfig(const Any& any, 
                                                             IndexPartitionSchemaImpl& schema)
{
    JsonMap dictionary = AnyCast<JsonMap>(any);

    //parse dictionary name
    JsonMap::iterator iter = dictionary.find(DICTIONARY_NAME);
    if (iter == dictionary.end())
    {
        INDEXLIB_FATAL_ERROR(Schema,
                           "dictionary_name absent in dictionary define");
    }
    string dictName = AnyCast<string>(iter->second);
    if (dictName.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "dictionary_name must not be empty");
    }

    //parse file name
    iter = dictionary.find(DICTIONARY_CONTENT);
    if (iter == dictionary.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "dictionary_file_name "
                           "absent in dictionary define");
    }
    string fileName = AnyCast<string>(iter->second);
    if (fileName.empty())
    {
        INDEXLIB_FATAL_ERROR(Schema, "dictionary_file_name must not be empty");
    }

    return schema.AddDictionaryConfig(dictName, fileName);
}

void SchemaConfigurator::LoadRegions(const Any& any,
                                     IndexPartitionSchemaImpl& schema)
{
    vector<RegionSchemaPtr> regions;
    JsonMap indexPartitionMap = AnyCast<JsonMap>(any);
    JsonMap::iterator it = indexPartitionMap.find(REGIONS);
    if (it == indexPartitionMap.end())
    {
        // legacy format
        RegionSchemaPtr regionSchema = LoadRegionSchema(any, schema, false);
        regions.push_back(regionSchema);
    }
    else
    {
        JsonArray regionVector = AnyCast<JsonArray>(it->second);
        for (JsonArray::iterator iter = regionVector.begin(); 
             iter != regionVector.end(); ++iter)
        {
            RegionSchemaPtr regionSchema = LoadRegionSchema(*iter, schema, true);
            regions.push_back(regionSchema);
        }
    }

    if (regions.size() > MAX_REGION_COUNT)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "region count [%lu] over MAX_REGION_COUNT [%d] limit",
                             regions.size(), MAX_REGION_COUNT);
    }
    
    schema.ResetRegions();
    for (size_t i = 0; i < regions.size(); i++)
    {
        schema.AddRegionSchema(regions[i]);
    }

    if (schema.GetTableType() == tt_kkv || schema.GetTableType() == tt_kv)
    {
        for (regionid_t i = 0; i < (regionid_t)schema.GetRegionCount(); i++)
        {
            const IndexSchemaPtr& indexSchema = schema.GetIndexSchema(i);
            assert(indexSchema);
            const SingleFieldIndexConfigPtr& pkConfig = indexSchema->GetPrimaryKeyIndexConfig();
            KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, pkConfig);
            assert(kvConfig);
            kvConfig->SetRegionInfo(i, schema.GetRegionCount());
        }
    }
}

void SchemaConfigurator::LoadModifyOperations(const Any& any,
        IndexPartitionSchemaImpl& schema)
{
    JsonArray modifyOpVec = AnyCast<JsonArray>(any);
    if (modifyOpVec.empty())
    {
        return;
    }

    if (schema.GetRegionCount() > 1 || schema.GetTableType() != tt_index) {
        INDEXLIB_FATAL_ERROR(UnSupported, "only index table with single region "
                             "schema support modify_operations");
    }
    if (schema.GetMaxModifyOperationCount() == 0) {
        return;
    }
    schema.SetBaseSchemaImmutable();
    for (JsonArray::iterator iter = modifyOpVec.begin(); 
         iter != modifyOpVec.end(); ++iter)
    {
        LoadOneModifyOperation(*iter, schema);
    }
    schema.SetModifySchemaImmutable();    
}

void SchemaConfigurator::LoadOneModifyOperation(const Any& any,
        IndexPartitionSchemaImpl& schema)
{
    uint32_t maxOpCount = schema.GetMaxModifyOperationCount();
    if (schema.GetModifyOperationCount() >= (size_t)maxOpCount)
    {
        IE_LOG(WARN, "reach max_modify_operation_count [%u], "
               "will ignore other modify operation", maxOpCount);
        return;
    }
        
    SchemaModifyOperationPtr modifyOp(new SchemaModifyOperation);
    const JsonMap& jsonMap = AnyCast<JsonMap>(any);
    auto it = jsonMap.find(SCHEMA_MODIFY_DEL);
    if (it != jsonMap.end())
    {
        modifyOp->LoadDeleteOperation(it->second, schema);
    }

    it = jsonMap.find(SCHEMA_MODIFY_ADD);
    if (it != jsonMap.end())
    {
        modifyOp->LoadAddOperation(it->second, schema);
    }

    it = jsonMap.find(SCHEMA_MODIFY_PARAMETER);
    if (it != jsonMap.end())
    {
        Jsonizable::JsonWrapper jsonWrapper(any);
        map<string, string> params;
        jsonWrapper.Jsonize(SCHEMA_MODIFY_PARAMETER, params);
        modifyOp->SetParams(params);
    }
    modifyOp->Validate();
    schema.AddSchemaModifyOperation(modifyOp);
}

RegionSchemaPtr SchemaConfigurator::LoadRegionSchema(
        const Any& any, IndexPartitionSchemaImpl& schema, bool multiRegionFormat)
{
    RegionSchemaPtr regionSchema(new RegionSchema(&schema, multiRegionFormat));
    Jsonizable::JsonWrapper jsonWrapper(any);
    regionSchema->Jsonize(jsonWrapper);
    return regionSchema;
}

void SchemaConfigurator::LoadCustomizedConfig(const Any& any,
                                              IndexPartitionSchemaImpl& schema)
{
    JsonMap indexPartitionMap = AnyCast<JsonMap>(any);
    //parse customized table config
    JsonMap::iterator iter = indexPartitionMap.find(CUSTOMIZED_TABLE_CONFIG);
    if (iter == indexPartitionMap.end())
    {
        INDEXLIB_FATAL_ERROR(Schema, "customized table must have %s", CUSTOMIZED_TABLE_CONFIG.c_str());
    }
    Jsonizable::JsonWrapper jsonWrapper(AnyCast<JsonMap>(iter->second));
    CustomizedTableConfigPtr mCustomizedTableConfig(new CustomizedTableConfig());
    mCustomizedTableConfig->Jsonize(jsonWrapper);
    schema.SetCustomizedTableConfig(mCustomizedTableConfig);

    // parse custoized document config
    iter = indexPartitionMap.find(CUSTOMIZED_DOCUMENT_CONFIG);
    if (iter != indexPartitionMap.end())
    {
        CustomizedConfigVector customizedDocumentConfigs;
        JsonArray documentConfigs = AnyCast<JsonArray>(iter->second);
        for (JsonArray::iterator docConfigIter = documentConfigs.begin(); 
             docConfigIter != documentConfigs.end(); ++docConfigIter)
        {
            JsonMap docConfigMap = AnyCast<JsonMap>(*docConfigIter);
            Jsonizable::JsonWrapper jsonWrapper(docConfigMap);
            CustomizedConfigPtr documentConfig(new CustomizedConfig());
            documentConfig->Jsonize(jsonWrapper);
            customizedDocumentConfigs.push_back(documentConfig);
        }
        schema.SetCustomizedDocumentConfig(customizedDocumentConfigs);
    }    
}

void SchemaConfigurator::AddOfflineJoinVirtualAttribute(
    const std::string& attrName, 
    config::IndexPartitionSchemaImpl& destSchema)
{
    //TODO: whether exist field, or user defined, check exist
    FieldSchemaPtr fieldSchemaPtr = destSchema.GetFieldSchema();
    if (fieldSchemaPtr
        && fieldSchemaPtr->IsFieldNameInSchema(attrName))
    {
        return;
    }

    destSchema.AddFieldConfig(attrName, ft_int32, false, false);
    destSchema.AddAttributeConfig(attrName);
}

IE_NAMESPACE_END(config);

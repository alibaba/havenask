#ifndef __INDEXLIB_INDEX_CONFIG_CREATOR_H
#define __INDEXLIB_INDEX_CONFIG_CREATOR_H

#include <tr1/memory>
#include <autil/legacy/json.h>
#include <autil/legacy/any.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/field_schema.h"
#include "indexlib/config/package_index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/dictionary_schema.h"
#include "indexlib/config/adaptive_dictionary_schema.h"

IE_NAMESPACE_BEGIN(config);

class IndexConfigCreator
{
public:
    IndexConfigCreator();
    ~IndexConfigCreator();

public:
    //to do replace any with string
    static IndexConfigPtr Create(const FieldSchemaPtr& fieldSchema,
                                 const DictionarySchemaPtr& dictSchema,
                                 const AdaptiveDictionarySchemaPtr& adaptDictSchema,
                                 const autil::legacy::Any& indexConfigJson,
                                 TableType tableType);

    static SingleFieldIndexConfigPtr CreateSingleFieldIndexConfig(
            const std::string& indexName, 
            IndexType indexType, 
            const std::string& fieldName, 
            const std::string& analyzerName,
            bool isVirtual,
            const FieldSchemaPtr& fieldSchema);

public:
    //public for test
    static PackageIndexConfigPtr CreatePackageIndexConfig(
            const std::string& indexName, 
            IndexType indexType, 
            const std::vector<std::string>& fieldNames, 
            const std::vector<int32_t> boosts, 
            const std::string& analyzerName,
            bool isVirtual,
            const FieldSchemaPtr& fieldSchema);

    static void CreateShardingIndexConfigs(
            const config::IndexConfigPtr& indexConfig, size_t shardingCount);

private:
    //static IndexConfigPtr CreateIndexConfig();
    static std::string GetIndexName(const autil::legacy::json::JsonMap& index);
    static std::string GetAnalyzerName(const autil::legacy::json::JsonMap& index);
    static IndexType GetIndexType(const autil::legacy::json::JsonMap& index, TableType tableType);
    static DictionaryConfigPtr GetDictConfig(
            const autil::legacy::json::JsonMap& index, 
            const DictionarySchemaPtr& dictSchema);
    static AdaptiveDictionaryConfigPtr GetAdaptiveDictConfig(
            const autil::legacy::json::JsonMap& index,
            const AdaptiveDictionarySchemaPtr& adaptiveDictSchema);
    static void LoadPackageIndexFields(const autil::legacy::json::JsonMap& index,
            const std::string& indexName,
            std::vector<std::string>& fields, 
            std::vector<int32_t>& boosts);
    static void LoadSingleFieldIndexField(
            const autil::legacy::json::JsonMap& index, 
            const std::string& indexName, 
            std::string& fieldName);
    static void CheckOptionFlag(IndexType indexType,
                                const autil::legacy::json::JsonMap& index);
    static optionflag_t CalcOptionFlag(const autil::legacy::json::JsonMap& indexMap, 
            IndexType indexType);
    static void updateOptionFlag(const autil::legacy::json::JsonMap& indexMap, 
                                 const std::string& optionType, uint8_t flag,
                                 optionflag_t& option);
    
    static bool GetShardingConfigInfo(
            const autil::legacy::json::JsonMap& index, 
            IndexType indexType, size_t &shardingCount);
    
    static IndexConfigPtr CreateKKVIndexConfig(
            const std::string& indexName,
            IndexType indexType,
            const autil::legacy::Any& indexConfigJson,
            const FieldSchemaPtr& fieldSchema);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexConfigCreator);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_CONFIG_CREATOR_H

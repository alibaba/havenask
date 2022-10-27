#ifndef __INDEXLIB_SCHEMA_MAKER_H
#define __INDEXLIB_SCHEMA_MAKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(testlib);

class SchemaMaker
{
public:
    typedef std::vector<std::string> StringVector;

    SchemaMaker() {}
    ~SchemaMaker() {}
public:
    static config::IndexPartitionSchemaPtr MakeSchema(
            const std::string& fieldNames, 
            const std::string& indexNames, 
            const std::string& attributeNames, 
            const std::string& summaryNames,
	    const std::string& truncateProfileStr = "");
    static config::IndexPartitionSchemaPtr MakeKKVSchema(
            const std::string& fieldNames,
            const std::string& pkeyName,
            const std::string& skeyName,
            const std::string& valueNames);
    static config::IndexPartitionSchemaPtr MakeKVSchema(
            const std::string& fieldNames,
            const std::string& keyName,
            const std::string& valueNames,
            int64_t ttl = INVALID_TTL);
    static void SetAdaptiveHighFrequenceDictionary(
            const std::string& indexName,
            const std::string& adaptiveRuleStr,
            HighFrequencyTermPostingType type,
            const config::IndexPartitionSchemaPtr& schema);

public:
    // for testlib
    static void MakeFieldConfigForSchema(config::IndexPartitionSchemaPtr& schema,
                                         const std::string& fieldNames)
    {
        MakeFieldConfigSchema(schema, SplitToStringVector(fieldNames));
    }

    static std::vector<config::IndexConfigPtr> MakeIndexConfigs(
            const std::string& indexNames,
            const config::IndexPartitionSchemaPtr& schema);
    
    static config::FieldSchemaPtr MakeFieldSchema(
            const std::string& fieldNames);
    
    static void MakeAttributeConfigForSchema(config::IndexPartitionSchemaPtr& schema,
            const std::string& fieldNames, regionid_t regionId = DEFAULT_REGIONID)
    {
        MakeAttributeConfigSchema(schema, SplitToStringVector(fieldNames), regionId);
    }

    static void MakeKKVIndex(config::IndexPartitionSchemaPtr& schema,
                             const std::string& pkeyName,
                             const std::string& skeyName,
                             regionid_t regionId = DEFAULT_REGIONID,
                             int64_t ttl = INVALID_TTL);

    static void MakeKVIndex(config::IndexPartitionSchemaPtr& schema,
                            const std::string& keyName,
                            regionid_t regionId = DEFAULT_REGIONID,
                            int64_t ttl = INVALID_TTL);
    
private:
    static void MakeFieldConfigSchema(config::IndexPartitionSchemaPtr& schema,
            const StringVector& fieldNames);

    static void MakeIndexConfigSchema(config::IndexPartitionSchemaPtr& schema,
            const std::string& indexNames);

    static void MakeAttributeConfigSchema(config::IndexPartitionSchemaPtr& schema,
            const StringVector& fieldNames, regionid_t regionId = DEFAULT_REGIONID);

    static void MakeSummaryConfigSchema(config::IndexPartitionSchemaPtr& schema,
            const StringVector& fieldNames);
    
    static void MakeTruncateProfiles(config::IndexPartitionSchemaPtr& schema,
				     const std::string& truncateProfileStr);

private:
    static StringVector SplitToStringVector(const std::string& names);

private:
    friend class ModifySchemaMaker;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaMaker);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_SCHEMA_MAKER_H

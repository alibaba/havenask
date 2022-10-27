#ifndef __INDEXLIB_INDEX_PARTITION_SCHEMA_MAKER_H
#define __INDEXLIB_INDEX_PARTITION_SCHEMA_MAKER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(config);

class IndexPartitionSchemaMaker
{
public:
    typedef std::vector<std::string> StringVector;

    IndexPartitionSchemaMaker();
    ~IndexPartitionSchemaMaker();
public:
    static void MakeSchema(IndexPartitionSchemaPtr& schema,
                           const std::string& fieldNames,
                           const std::string& indexNames,
                           const std::string& attributeNames,
                           const std::string& summaryNames,
                           const std::string& truncateProfileStr = "");

    static void MakeFieldConfigSchema(IndexPartitionSchemaPtr& schema,
            const StringVector& fieldNames);

    static void MakeIndexConfigSchema(IndexPartitionSchemaPtr& schema,
            const StringVector& fieldNames);

    static void MakeAttributeConfigSchema(IndexPartitionSchemaPtr& schema,
            const StringVector& fieldNames);

    static void MakeSummaryConfigSchema(IndexPartitionSchemaPtr& schema,
            const StringVector& fieldNames);

    static void MakeTruncateProfiles(IndexPartitionSchemaPtr& schema,
				     const std::string& truncateProfileStr);

    static StringVector SpliteToStringVector(const std::string& names);
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<IndexPartitionSchemaMaker> IndexPartitionSchemaMakerPtr;

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_INDEX_PARTITION_SCHEMA_MAKER_H

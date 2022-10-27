#ifndef __INDEXLIB_INDEXLIB_PARTITION_CREATOR_H
#define __INDEXLIB_INDEXLIB_PARTITION_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"

IE_NAMESPACE_BEGIN(testlib);

class IndexlibPartitionCreator
{
public:
    IndexlibPartitionCreator();
    ~IndexlibPartitionCreator();

public:
    void Init(const std::string& tableName,
              const std::string& fieldNames,
              const std::string& indexNames,
              const std::string& attributeNames,
              const std::string& summaryNames,
              const std::string& truncateProfileStr = "");

    partition::IndexPartitionPtr CreateIndexPartition(
        const std::string& rootPath,
        const std::string& docStr);

public:
    static config::IndexPartitionSchemaPtr CreateSchema(
            const std::string& tableName,
            const std::string& fieldNames,
            const std::string& indexNames,
            const std::string& attributeNames,
            const std::string& summaryNames,
            const std::string& truncateProfileStr = "");

    static config::IndexPartitionSchemaPtr CreateKKVSchema(
            const std::string& tableName,
            const std::string& fieldNames,
            const std::string& pkeyField,
            const std::string& skeyField,
            const std::string& valueNames);

    // regionInfos:
    // regionName#pkey#skey#valueNames|regionName#pkey#skey#valueNames|...
    static config::IndexPartitionSchemaPtr CreateMultiRegionKKVSchema(
            const std::string& tableName,
            const std::string& fieldNames,
            const std::string& regionInfos);

    static config::IndexPartitionSchemaPtr CreateKVSchema(
            const std::string& tableName,
            const std::string& fieldNames, 
            const std::string& keyName,
            const std::string& valueNames,
            bool enableTTL = false);

    // regionInfos:
    // regionName#key#valueNames|regionName#key#valueNames|...
    static config::IndexPartitionSchemaPtr CreateMultiRegionKVSchema(
            const std::string& tableName,
            const std::string& fieldNames,
            const std::string& regionInfos);

    static config::IndexPartitionSchemaPtr CreateSchemaWithSub(
        const config::IndexPartitionSchemaPtr& mainSchema,
        const config::IndexPartitionSchemaPtr& subSchema);

    static partition::IndexPartitionPtr CreateIndexPartition(
        const config::IndexPartitionSchemaPtr& schema,
            const std::string& rootPath,
            const std::string& docStr,
            const config::IndexPartitionOptions& options = 
            config::IndexPartitionOptions(),
            const std::string& indexPluginPath = "",
            bool needMerge = true);
    static void BuildParallelIncData(
        const config::IndexPartitionSchemaPtr& schema,
        const std::string& rootPath,
        const index_base::ParallelBuildInfo& parallelInfo,
        const std::string& docStr,
        const config::IndexPartitionOptions& options =
        config::IndexPartitionOptions(),
        int64_t stopTs = INVALID_TIMESTAMP);
    static void AddRtDocsToPartition(const partition::IndexPartitionPtr& partition,
            const std::string& docStr);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexlibPartitionCreator);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_INDEXLIB_PARTITION_CREATOR_H

#ifndef __INDEXLIB_INDEXLIB_PARTITION_CREATOR_H
#define __INDEXLIB_INDEXLIB_PARTITION_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition.h"

namespace indexlib { namespace testlib {

class IndexlibPartitionCreator
{
public:
    enum ImpactValueTestMode {
        IVT_ENABLE,
        IVT_DISABLE,
        IVT_RANDOM,
    };

public:
    IndexlibPartitionCreator();
    ~IndexlibPartitionCreator();

public:
    void Init(const std::string& tableName, const std::string& fieldNames, const std::string& indexNames,
              const std::string& attributeNames, const std::string& summaryNames,
              const std::string& truncateProfileStr = "");

    partition::IndexPartitionPtr CreateIndexPartition(const std::string& rootPath, const std::string& docStr);

public:
    static config::IndexPartitionSchemaPtr CreateSchema(const std::string& tableName, const std::string& fieldNames,
                                                        const std::string& indexNames,
                                                        const std::string& attributeNames,
                                                        const std::string& summaryNames,
                                                        const std::string& truncateProfileStr = "");

    static config::IndexPartitionSchemaPtr CreateKKVSchema(const std::string& tableName, const std::string& fieldNames,
                                                           const std::string& pkeyField, const std::string& skeyField,
                                                           const std::string& valueNames, bool enableTTL = false,
                                                           int64_t ttl = std::numeric_limits<int64_t>::max());

    // regionInfos:
    // regionName#pkey#skey#valueNames|regionName#pkey#skey#valueNames|...
    static config::IndexPartitionSchemaPtr CreateMultiRegionKKVSchema(const std::string& tableName,
                                                                      const std::string& fieldNames,
                                                                      const std::string& regionInfos);

    static config::IndexPartitionSchemaPtr CreateKVSchema(const std::string& tableName, const std::string& fieldNames,
                                                          const std::string& keyName, const std::string& valueNames,
                                                          bool enableTTL = false,
                                                          int64_t ttl = std::numeric_limits<int64_t>::max());

    // regionInfos:
    // regionName#key#valueNames|regionName#key#valueNames|...
    static config::IndexPartitionSchemaPtr CreateMultiRegionKVSchema(const std::string& tableName,
                                                                     const std::string& fieldNames,
                                                                     const std::string& regionInfos);

    static config::IndexPartitionSchemaPtr CreateSchemaWithSub(const config::IndexPartitionSchemaPtr& mainSchema,
                                                               const config::IndexPartitionSchemaPtr& subSchema);

    static partition::IndexPartitionPtr
    CreateIndexPartition(const config::IndexPartitionSchemaPtr& schema, const std::string& rootPath,
                         const std::string& docStr,
                         const config::IndexPartitionOptions& options = config::IndexPartitionOptions(),
                         const std::string& indexPluginPath = "", bool needMerge = true,
                         const indexlibv2::config::SortDescriptions& sortDescriptions = {});
    static void BuildParallelIncData(const config::IndexPartitionSchemaPtr& schema, const std::string& rootPath,
                                     const index_base::ParallelBuildInfo& parallelInfo, const std::string& docStr,
                                     const config::IndexPartitionOptions& options = config::IndexPartitionOptions(),
                                     int64_t stopTs = INVALID_TIMESTAMP, uint32_t branchId = 0);
    static void AddRtDocsToPartition(const partition::IndexPartitionPtr& partition, const std::string& docStr);

    static void SetImpactValueTestMode(ImpactValueTestMode mode);
    static bool IsImpactValueFormat();

private:
    config::IndexPartitionSchemaPtr mSchema;
    static ImpactValueTestMode IMPACT_VALUE_TEST_MODE;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexlibPartitionCreator);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_INDEXLIB_PARTITION_CREATOR_H

#ifndef __INDEXLIB_INDEX_PARTITION_CREATOR_H
#define __INDEXLIB_INDEX_PARTITION_CREATOR_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartition);
DECLARE_REFERENCE_CLASS(partition, IndexPartitionResource);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(util, MemoryQuotaController);

IE_NAMESPACE_BEGIN(partition);

class IndexPartitionCreator
{
public:
    IndexPartitionCreator();
    ~IndexPartitionCreator();

public:
    static IndexPartitionPtr Create(
            const config::IndexPartitionOptions& options,
            const util::MemoryQuotaControllerPtr& memController,
            const std::string& partitionName,
            misc::MetricProviderPtr metricProvider);

    static IndexPartitionPtr Create(const std::string& partitionName,
                                    const config::IndexPartitionOptions& options,
                                    const IndexPartitionResource& indexPartitionResource);

    static IndexPartitionPtr CreateCustomIndexPartition(
        const std::string& partitionName,
        const config::IndexPartitionOptions& options,
        const IndexPartitionResource& indexPartitionResource);

    static IndexPartitionPtr Create(const std::string& partitionName,
                                    const config::IndexPartitionOptions& options,
                                    const IndexPartitionResource& indexPartitionResource,
                                    const std::string& schemaRoot,
                                    versionid_t versionId = INVALID_VERSION);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartitionCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_PARTITION_CREATOR_H

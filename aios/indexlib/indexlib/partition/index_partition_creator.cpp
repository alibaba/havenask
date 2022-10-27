#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/custom_offline_partition.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/index_partition_options.h"

using namespace std;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, IndexPartitionCreator);

IndexPartitionCreator::IndexPartitionCreator() 
{
}

IndexPartitionCreator::~IndexPartitionCreator() 
{
}

IndexPartitionPtr IndexPartitionCreator::Create(
        const IndexPartitionOptions& options,
        const MemoryQuotaControllerPtr& memController,
        const string& partitionName, MetricProviderPtr metricProvider)
{
    if (options.IsOnline())
    {
        return IndexPartitionPtr(new OnlinePartition(
                        partitionName, memController, metricProvider));
    }
    assert(options.IsOffline());
    return IndexPartitionPtr(new OfflinePartition(
                    partitionName, memController, metricProvider));
}

IndexPartitionPtr IndexPartitionCreator::Create(const string& partitionName,
                                                const IndexPartitionOptions& options,
                                                const IndexPartitionResource& indexPartitionResource)
{
    if (options.IsOnline())
    {
        return IndexPartitionPtr(new OnlinePartition(
                        partitionName, indexPartitionResource));
    }
    assert(options.IsOffline());
    return IndexPartitionPtr(new OfflinePartition(
                    partitionName, indexPartitionResource));
}

IndexPartitionPtr IndexPartitionCreator::CreateCustomIndexPartition(
    const string& partitionName,
    const IndexPartitionOptions& options,
    const IndexPartitionResource& indexPartitionResource)
{
    if (options.IsOnline())
    {
        return IndexPartitionPtr(new CustomOnlinePartition(
                        partitionName, indexPartitionResource));
    }
    assert(options.IsOffline());
    return IndexPartitionPtr(new CustomOfflinePartition(
                    partitionName, indexPartitionResource));    
}

IndexPartitionPtr IndexPartitionCreator::Create(
        const string& partitionName, const IndexPartitionOptions& options,
        const IndexPartitionResource& indexPartitionResource, const string& schemaRoot,
        versionid_t versionId)
{
    try
    {
        auto schema = index_base::SchemaAdapter::LoadSchemaForVersion(schemaRoot, versionId);
        if (!schema)
        {
            IE_LOG(ERROR, "load schema from [%s] failed", schemaRoot.c_str());
            return IndexPartitionPtr();
        }

        if (schema->GetTableType() == tt_customized)
        {
            return CreateCustomIndexPartition(partitionName, options, indexPartitionResource);
        }
        else
        {
            return Create(partitionName, options, indexPartitionResource);
        }
    }
    catch (const exception& e)
    {
        IE_LOG(ERROR, "caught exception: %s", e.what());
    }
    catch (...)
    {
        IE_LOG(ERROR, "caught unknown exception");
    }
    return IndexPartitionPtr();
}

IE_NAMESPACE_END(partition);


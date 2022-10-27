#ifndef __INDEXLIB_INDEX_PARTITION_PATCHER_H
#define __INDEXLIB_INDEX_PARTITION_PATCHER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/attribute_data_patcher.h"
#include "indexlib/partition/remote_access/index_data_patcher.h"
#include "indexlib/config/merge_io_config.h"

DECLARE_REFERENCE_CLASS(partition, OfflinePartition);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

IE_NAMESPACE_BEGIN(partition);

class PartitionPatcher
{
private:
    static const size_t DEFAULT_INDEX_INIT_TERM_COUNT = 4 * 1024;

public:
    PartitionPatcher() {}
    ~PartitionPatcher() {}

public:
    bool Init(const OfflinePartitionPtr& offlinePartition,
              const config::IndexPartitionSchemaPtr& newSchema,
              const std::string& patchDir,
              const plugin::PluginManagerPtr& pluginManager);

    AttributeDataPatcherPtr CreateSingleAttributePatcher(
            const std::string& attrName, segmentid_t segmentId);

    IndexDataPatcherPtr CreateSingleIndexPatcher(
            const std::string& indexName, segmentid_t segmentId,
            size_t initDistinctTermCount = DEFAULT_INDEX_INIT_TERM_COUNT);

private:
    config::AttributeConfigPtr GetAlterAttributeConfig(const std::string& attrName) const;
    config::IndexConfigPtr GetAlterIndexConfig(const std::string& indexName) const;

private:
    config::IndexPartitionSchemaPtr mOldSchema;
    config::IndexPartitionSchemaPtr mNewSchema;
    config::MergeIOConfig mMergeIOConfig;
    std::vector<config::AttributeConfigPtr> mAlterAttributes;
    std::vector<config::IndexConfigPtr> mAlterIndexes;
    index_base::PartitionDataPtr mPartitionData;
    std::string mPatchDir;
    plugin::PluginManagerPtr mPluginManager;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionPatcher);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_PARTITION_PATCHER_H

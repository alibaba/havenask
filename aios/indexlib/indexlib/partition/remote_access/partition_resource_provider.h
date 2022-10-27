#ifndef __INDEXLIB_PARTITION_RESOURCE_PROVIDER_H
#define __INDEXLIB_PARTITION_RESOURCE_PROVIDER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/partition_seeker.h"
#include "indexlib/partition/remote_access/partition_iterator.h"
#include "indexlib/partition/remote_access/partition_patcher.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/config/index_partition_options.h"

DECLARE_REFERENCE_CLASS(partition, OfflinePartition);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
IE_NAMESPACE_BEGIN(partition);

class PartitionResourceProvider
{
public:
    PartitionResourceProvider(const config::IndexPartitionOptions& options);

    ~PartitionResourceProvider() {}

public:
    bool Init(const std::string& indexPath, versionid_t targetVersionId,
              const std::string& pluginPath);

    PartitionIteratorPtr CreatePartitionIterator();

    PartitionSeekerPtr CreatePartitionSeeker();

    PartitionPatcherPtr CreatePartitionPatcher(
            const config::IndexPartitionSchemaPtr& newSchema,
            const std::string& patchDir);

    IndexPartitionReaderPtr GetReader() const;
    std::string GetPatchRootDirName(
            const config::IndexPartitionSchemaPtr& newSchema);
    
    void StoreVersion(const config::IndexPartitionSchemaPtr& newSchema,
                      versionid_t versionId);
    
    const config::IndexPartitionSchemaPtr& GetSchema() const { return mSchema; }
    const index_base::Version& GetVersion() const { return mVersion; }
    const std::string& GetIndexPath() const { return mPath; }

    index_base::PartitionSegmentIteratorPtr CreatePartitionSegmentIterator() const;
    OfflinePartitionPtr GetPartition() const {
        return mOfflinePartition;
    }
    void DumpDeployIndexForPatchSegments(
            const config::IndexPartitionSchemaPtr& newSchema);
private:
    int32_t GetNewSchemaVersionId(const config::IndexPartitionSchemaPtr& newSchema);
    index_base::Version GetNewVersion(const config::IndexPartitionSchemaPtr& newSchema,
                                 versionid_t versionId);

private:
    typedef std::pair<int32_t, plugin::PluginManagerPtr> NewSchemaPluginManagerPair;
    OfflinePartitionPtr mOfflinePartition;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
    index_base::Version mVersion;
    std::string mPath;
    std::string mPluginPath;
    NewSchemaPluginManagerPair mPluginManagerPair;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionResourceProvider);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_RESOURCE_PROVIDER_H

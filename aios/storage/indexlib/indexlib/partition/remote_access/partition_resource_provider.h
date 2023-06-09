/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_PARTITION_RESOURCE_PROVIDER_H
#define __INDEXLIB_PARTITION_RESOURCE_PROVIDER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/partition_iterator.h"
#include "indexlib/partition/remote_access/partition_patcher.h"
#include "indexlib/partition/remote_access/partition_seeker.h"

DECLARE_REFERENCE_CLASS(partition, OfflinePartition);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace partition {

class PartitionResourceProvider
{
public:
    PartitionResourceProvider(const config::IndexPartitionOptions& options);

    ~PartitionResourceProvider();

public:
    bool Init(const std::string& indexPath, versionid_t targetVersionId, const std::string& pluginPath);
    void Sync();

    PartitionIteratorPtr CreatePartitionIterator();

    PartitionSeekerPtr CreatePartitionSeeker();

    PartitionPatcherPtr CreatePartitionPatcher(const config::IndexPartitionSchemaPtr& newSchema,
                                               const file_system::DirectoryPtr& patchDir);

    IndexPartitionReaderPtr GetReader() const;
    std::string GetPatchRootDirName(const config::IndexPartitionSchemaPtr& newSchema);

    void StoreVersion(const config::IndexPartitionSchemaPtr& newSchema, versionid_t versionId);

    const config::IndexPartitionSchemaPtr& GetSchema() const { return mSchema; }
    const index_base::Version& GetVersion() const { return mVersion; }
    const std::string& GetIndexPath() const { return mPath; }

    index_base::PartitionSegmentIteratorPtr CreatePartitionSegmentIterator() const;
    OfflinePartitionPtr GetPartition() const { return mOfflinePartition; }
    void DumpDeployIndexForPatchSegments(const config::IndexPartitionSchemaPtr& newSchema);

public:
    // used in BS, for AlterField's EndMerge
    void MountPatchIndex(const std::string& physicalRoot, const std::string& patchDir);

private:
    int32_t GetNewSchemaVersionId(const config::IndexPartitionSchemaPtr& newSchema);
    index_base::Version GetNewVersion(const config::IndexPartitionSchemaPtr& newSchema, versionid_t versionId);
    bool EnsurePartition(bool isSekeer) const;

private:
    typedef std::pair<int32_t, plugin::PluginManagerPtr> NewSchemaPluginManagerPair;
    versionid_t mTargetVersionId;
    mutable OfflinePartitionPtr mOfflinePartition;
    config::IndexPartitionOptions mOptions;
    mutable config::IndexPartitionOptions mCurrentOptions;
    mutable config::IndexPartitionSchemaPtr mSchema;
    mutable index_base::Version mVersion;
    std::string mPath;
    std::string mPluginPath;
    mutable file_system::DirectoryPtr mDirectory;
    file_system::DirectoryPtr mPatchIndexDir;
    NewSchemaPluginManagerPair mPluginManagerPair;
    bool mIsMergeInstance = {false};

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionResourceProvider);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_RESOURCE_PROVIDER_H

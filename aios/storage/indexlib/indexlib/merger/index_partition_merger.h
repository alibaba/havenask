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
#ifndef __INDEXLIB_INDEX_PARTITION_MERGER_H
#define __INDEXLIB_INDEX_PARTITION_MERGER_H

#include <assert.h>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "autil/LoopThread.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/segment_metrics_updater/segment_metrics_updater.h"
#include "indexlib/index_base/common_branch_hinter.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/merger/merge_scheduler.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/merger/partition_merger.h"
#include "indexlib/merger/segment_directory.h"

DECLARE_REFERENCE_CLASS(merger, MergeWorkItemCreator);
DECLARE_REFERENCE_CLASS(index, ReclaimMapCreator);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace merger {
class IndexPartitionMerger : public PartitionMerger
{
public:
    static const std::string MERGE_INSTANCE_DIR_PREFIX;
    static const std::string INSTANCE_TARGET_VERSION_FILE;

public:
    IndexPartitionMerger(const SegmentDirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema,
                         const config::IndexPartitionOptions& options, const DumpStrategyPtr& dumpStrategy,
                         util::MetricProviderPtr metricProvider, const plugin::PluginManagerPtr& pluginManager,
                         const index_base::CommonBranchHinterOption& branchOption,
                         const PartitionRange& range = PartitionRange());

protected:
    // for inc parallel build merger
    IndexPartitionMerger() {};

public:
    virtual ~IndexPartitionMerger() {};

public:
    /**
     * Note:
     *      virtual for test
     * @return false, when no merge happens, true when merge some segments
     * @throw BadParameterException if merge plan is invalid.
     * FileIOException if some file missed.
     */
    virtual bool Merge(bool optimize = false, int64_t currentTs = 0);
    virtual index_base::Version GetMergedVersion() const;

public:
    // separate mode.
    virtual MergeMetaPtr CreateMergeMeta(bool optimize, uint32_t instanceCount, int64_t currentTs);
    virtual MergeMetaPtr LoadMergeMeta(const std::string& mergeMetaPath, bool onlyLoadBasicInfo);
    virtual void PrepareMerge(int64_t currentTs);
    virtual void DoMerge(bool optimize, const MergeMetaPtr& mergeMeta, uint32_t instanceId);

    virtual void EndMerge(const MergeMetaPtr& mergeMeta, versionid_t alignVersionId = INVALID_VERSION);

    virtual std::string GetMergeIndexRoot() const;
    virtual file_system::DirectoryPtr GetMergeIndexRootDirectory() const;
    virtual std::string GetMergeMetaDir() const;
    virtual std::string GetMergeCheckpointDir() const;

    virtual config::IndexPartitionSchemaPtr GetSchema() const { return mSchema; }

    virtual util::CounterMapPtr GetCounterMap() { return mCounterMap; }
    virtual const config::IndexPartitionOptions& GetOptions() const { return mOptions; }
    virtual MergeFileSystemPtr CreateMergeFileSystem(uint32_t instanceId, const MergeMetaPtr& mergeMeta);
    const PartitionRange& GetPartitionRange() const { return mPartitionRange; }
    static std::string GetInstanceMergeDir(const std::string& root, uint32_t instanceId);
    virtual void SetCheckpointRootDir(const std::string& checkpoint);
    virtual const index_base::CommonBranchHinterOption& GetBranchOption() const { return mHinter->GetOption(); }

    virtual index_base::BranchFS* TEST_GetBranchFs() const { return mBranchFs.get(); }

protected:
    virtual bool IsSortMerge() const { return false; }
    index_base::SegmentMergeInfos CreateSegmentMergeInfos(const SegmentDirectoryPtr& segDir) const;

    void ReclaimDocuments(const SegmentDirectoryPtr& segDir, int64_t currentTs);

    MergeSchedulerPtr CreateMergeScheduler(int64_t maxMemUse, uint32_t maxThreadNum);
    void AlignVersion(const file_system::DirectoryPtr& rootDirectory, versionid_t alignVersion);
    void SinglePartEndMerge(const file_system::DirectoryPtr& rootDirectory,
                            const std::vector<merger::MergePlan>& mergePlanMetas,
                            const index_base::Version& targetVersion);

    void ParallelEndMerge(const file_system::DirectoryPtr& rootDirectory, const IndexMergeMetaPtr& mergeMeta);
    static file_system::FileList ListInstanceDirNames(const std::string& rootPath);

private:
    virtual merger::MergeWorkItemCreatorPtr
    CreateMergeWorkItemCreator(bool optimize, const merger::IndexMergeMeta& mergeMeta,
                               const merger::MergeFileSystemPtr& mergeFileSystem);
    virtual index::ReclaimMapCreatorPtr CreateReclaimMapCreator();
    std::string GetCheckpointDirForPhase(const std::string& phase) const;

private:
    merger::IndexMergeMeta* CreateMergeMetaWithTask(MergeTask& task, const index_base::SegmentMergeInfos& segMergeInfos,
                                                    bool optimize, uint32_t instanceCount);

    void CleanTargetDirs(const MergeMetaPtr& mergeMeta);

    void MergeInstanceDir(const file_system::DirectoryPtr& rootDirectory,
                          const std::vector<merger::MergePlan>& mergePlanMetas,
                          const index_base::Version& targetVersion, const std::string& counterMapContent, bool hasSub);
    void MergeSegmentDir(const file_system::DirectoryPtr& rootDirectory,
                         const file_system::DirectoryPtr& segmentDirectory, const std::string& segmentPath);

    framework::SegmentMetrics CollectSegmentMetrics(const file_system::DirectoryPtr& targetSegmentDir,
                                                    const merger::MergePlan& plan, size_t targetSegIdx);

    void StoreSegmentInfos(const merger::MergePlan& mergePlanMeta, size_t targetSegIdx,
                           const file_system::DirectoryPtr& mergedDirectory, bool hasSub,
                           const std::string& counterMapContent, const framework::SegmentMetrics& segmentMetrics);
    void PrepareMergeDir(uint32_t instanceId, const MergeMetaPtr& mergeMeta, std::string& instanceRootPath);
    bool IsTargetVersionValid(versionid_t targetVersion);
    void ReCalculatorTemperature(index_base::SegmentMergeInfos& segMergeInfos, const std::string& checkpointDir);

private:
    static void SplitFileName(const std::string& input, std::string& folder, std::string& fileName);

protected:
    versionid_t GetTargetVersionId(const index_base::Version& version, versionid_t alignVersionId);

    // for test
    virtual MergeTask CreateMergeTaskByStrategy(bool optimize, const config::MergeConfig& mergeConfig,
                                                const index_base::SegmentMergeInfos& segMergeInfos,
                                                const indexlibv2::framework::LevelInfo& levelInfo) const;
    // use for branch fs
    void CommitToDefaultBranch(uint32_t instanceId) const;
    void CreateBranchProgressLoop();
    void SyncBranchProgress();

private:
    static void DeployPartitionInfo(const file_system::DirectoryPtr& rootDir);

    void TEST_MERGE_WITH_CHECKPOINTS(bool optimize, const MergeMetaPtr& mergeMeta, uint32_t instanceId);

protected:
    SegmentDirectoryPtr mSegmentDirectory;
    SegmentDirectoryPtr mSubPartSegmentDirectory;
    file_system::IFileSystemPtr mFileSystem;
    std::unique_ptr<index_base::BranchFS> mBranchFs;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    config::MergeConfig mMergeConfig;
    uint32_t mKeepVersionCount;
    DumpStrategyPtr mDumpStrategy;
    merger::IndexPartitionMergerMetrics mMetrics;
    util::CounterMapPtr mCounterMap;
    plugin::PluginManagerPtr mPluginManager;
    index::OfflineAttributeSegmentReaderContainerPtr mAttrReaderContainer;
    util::MetricProviderPtr mMetricProvider;
    PartitionRange mPartitionRange;
    file_system::FileSystemOptions mFsOptions;
    // for segment splist & calculate doc temperature
    std::map<segmentid_t, index::SegmentMetricsUpdaterPtr> mUpdaters;
    index::DeletionMapReaderPtr mDeletionReader;
    autil::LoopThreadPtr mSyncBranchProgressThread;
    int64_t mLastProgress;
    index_base::CommonBranchHinterPtr mHinter;
    std::string mCheckpointRootDir;

private:
    friend class MergeMetaWorkItem;
    friend class IndexPartitionMergerTest;
    friend class AttributeUpdateInteTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPartitionMerger);

//////////////////////////////////////////

inline index_base::Version IndexPartitionMerger::GetMergedVersion() const
{
    assert(mDumpStrategy);
    return mDumpStrategy->GetVersion();
}
}} // namespace indexlib::merger

#endif //__INDEXLIB_INDEX_PARTITION_MERGER_H

#ifndef __INDEXLIB_INDEX_PARTITION_MERGER_H
#define __INDEXLIB_INDEX_PARTITION_MERGER_H

#include <assert.h>
#include <vector>
#include <string>
#include <tr1/memory>
#include <tr1/unordered_set>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/merger/merge_file_system.h"
#include "indexlib/merger/merge_task.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger.h"
#include "indexlib/index/normal/deletionmap/deletion_map_merger.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/merger/merge_scheduler.h"
#include "indexlib/merger/partition_merger.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/merger/index_merge_meta.h"
#include "indexlib/merger/index_partition_merger_metrics.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

DECLARE_REFERENCE_CLASS(merger, MergeWorkItemCreator);
DECLARE_REFERENCE_CLASS(index, ReclaimMapCreator);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(merger);

class IndexPartitionMerger : public PartitionMerger
{
public:
    static const std::string MERGE_INSTANCE_DIR_PREFIX;
    static const std::string INSTANCE_TARGET_VERSION_FILE;

public:
    IndexPartitionMerger(const SegmentDirectoryPtr& segDir, 
                         const config::IndexPartitionSchemaPtr& schema,
                         const config::IndexPartitionOptions& options,
                         const DumpStrategyPtr &dumpStrategy,
                         misc::MetricProviderPtr metricProvider,
                         const plugin::PluginManagerPtr& pluginManager,
                         const PartitionRange& range = PartitionRange());
    // for inc parallel build merger
    IndexPartitionMerger();
    virtual ~IndexPartitionMerger();

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
    virtual MergeMetaPtr CreateMergeMeta(
        bool optimize, uint32_t instanceCount, int64_t currentTs);
    virtual MergeMetaPtr LoadMergeMeta(
        const std::string& mergeMetaPath, bool onlyLoadBasicInfo);
    virtual void PrepareMerge(int64_t currentTs);
    virtual void DoMerge(bool optimize,
                         const MergeMetaPtr& mergeMeta, 
                         uint32_t instanceId);

    virtual void EndMerge(const MergeMetaPtr& mergeMeta,
                          versionid_t alignVersionId = INVALID_VERSION);

    virtual std::string GetMergeIndexRoot() const;
    virtual std::string GetMergeMetaDir() const;

    virtual config::IndexPartitionSchemaPtr GetSchema() const
    {  return mSchema; }

    virtual util::CounterMapPtr GetCounterMap() { return mCounterMap; }
    virtual const config::IndexPartitionOptions& GetOptions() const { return mOptions; }

public:
    const PartitionRange &GetPartitionRange() const { return mPartitionRange; }

protected:
    virtual bool IsSortMerge() const { return false; }

    index_base::SegmentMergeInfos CreateSegmentMergeInfos(
            const SegmentDirectoryPtr &segDir) const;

    void ReclaimDocuments(const SegmentDirectoryPtr &segDir, int64_t currentTs);

    MergeSchedulerPtr CreateMergeScheduler(int64_t maxMemUse, uint32_t maxThreadNum);
    void AlignVersion(const std::string& rootDir, versionid_t alignVersion);
    void SinglePartEndMerge(const std::string &rootPath, 
                            const std::vector<merger::MergePlan> &mergePlanMetas,
                            const index_base::Version &targetVersion);

    void ParallelEndMerge(const file_system::DirectoryPtr& rootDirectory, const IndexMergeMetaPtr& mergeMeta);

    MergeFileSystemPtr CreateMergeFileSystem(const std::string& rootPath,
            uint32_t instanceId, const MergeMetaPtr& mergeMeta);
    
protected:
    static fslib::FileList ListInstanceDirs(const std::string &rootPath);
    static void MkDirIfNotExist(const std::string &dir);

private:
    virtual merger::MergeWorkItemCreatorPtr CreateMergeWorkItemCreator(
        bool optimize, const merger::IndexMergeMeta &mergeMeta,
        const merger::MergeFileSystemPtr& mergeFileSystem);
    virtual index::ReclaimMapCreatorPtr CreateReclaimMapCreator();

private:
    merger::IndexMergeMeta* CreateMergeMetaWithTask(MergeTask &task,
            const index_base::SegmentMergeInfos &segMergeInfos,
            bool optimize, uint32_t instanceCount);
    
    void CleanTargetDirs(const MergeMetaPtr& mergeMeta);

    void MergeInstanceDir(const file_system::DirectoryPtr& rootDirectory,
                          const std::vector<merger::MergePlan> &mergePlanMetas,
                          const index_base::Version &targetVersion,
                          const std::string& counterMapContent,
                          bool hasSub);
    index_base::SegmentMetrics CollectSegmentMetrics(
        const file_system::DirectoryPtr& targetSegmentDir, const merger::MergePlan& plan,
        size_t targetSegIdx);
    
    void StoreSegmentInfos(const merger::MergePlan& mergePlanMeta, size_t targetSegIdx, const std::string& mergedDir,
        bool hasSub, const std::string& counterMapContent,
        const index_base::SegmentMetrics& segmentMetrics);
    void PrepareMergeDir(const std::string &rootDir, uint32_t instanceId,
                         const MergeMetaPtr& mergeMeta, std::string &instanceRootPath);
    bool IsTargetVersionValid(versionid_t targetVersion);
private:
    static fslib::FileList ListInstanceFiles(const std::string &instanceDir,
            const std::string &subDir);
    static void MoveFiles(const fslib::FileList &files, const std::string &targetDir);
    static void SplitFileName(
            const std::string &input, std::string &folder, std::string &fileName);
    static std::string GetInstanceTempMergeDir(
            const std::string &rootDir, uint32_t instanceId);

protected:
    versionid_t GetTargetVersionId(const index_base::Version& version, versionid_t alignVersionId);

    // for test
    virtual MergeTask CreateMergeTaskByStrategy(
        bool optimize, const config::MergeConfig& mergeConfig,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::LevelInfo& levelInfo) const;
    
private:
    static void DeploySegmentIndex(const std::string& rootPath,
        const std::string& targetSegDirName, const std::string& lifecycle);
    static void DeployPartitionInfo(const std::string& rootPath);

    void TEST_MERGE_WITH_CHECKPOINTS(bool optimize,
            const MergeMetaPtr &mergeMeta, uint32_t instanceId);

protected:
    SegmentDirectoryPtr mSegmentDirectory;
    SegmentDirectoryPtr mSubPartSegmentDirectory;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    config::MergeConfig mMergeConfig;
    uint32_t mKeepVersionCount;
    DumpStrategyPtr mDumpStrategy;
    merger::IndexPartitionMergerMetrics mMetrics;
    util::CounterMapPtr mCounterMap;
    plugin::PluginManagerPtr mPluginManager;
    index::OfflineAttributeSegmentReaderContainerPtr mAttrReaderContainer;
    misc::MetricProviderPtr mMetricProvider;
    PartitionRange mPartitionRange;

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

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEX_PARTITION_MERGER_H

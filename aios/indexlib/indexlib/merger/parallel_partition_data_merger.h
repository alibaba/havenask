#ifndef __INDEXLIB_PARALLEL_PARTITION_DATA_MERGER_H
#define __INDEXLIB_PARALLEL_PARTITION_DATA_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/patch/patch_file_finder.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, ParallelBuildInfo);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, StateCounter);
DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);

IE_NAMESPACE_BEGIN(merger);

class ParallelPartitionDataMerger
{
public:
    ParallelPartitionDataMerger(const std::string& mergePath,
                                const config::IndexPartitionSchemaPtr& schema,
                                const config::IndexPartitionOptions& options = config::IndexPartitionOptions(),
                                const file_system::IndexlibFileSystemPtr& fileSystem = file_system::IndexlibFileSystemPtr());
    ~ParallelPartitionDataMerger();
public:
    index_base::Version MergeSegmentData(const std::vector<std::string>& srcPaths);
    void RemoveParallBuildDirectory();

    static std::vector<std::string> GetParallelInstancePaths(
            const std::string& rootPath, const index_base::ParallelBuildInfo& info);

    static std::vector<index_base::Version> GetVersionList(const std::vector<std::string>& srcPaths);
    
private:
    typedef std::pair<index_base::Version, size_t> VersionPair;
    // return base version id
    versionid_t GetBaseVersionIdFromParallelBuildInfo(const std::vector<std::string>& srcPaths);
    
    static bool NoNeedMerge(const std::vector<VersionPair>& versionInfos,
                            const index_base::Version& lastVersion, versionid_t baseVersionId);
    
    void MoveParallelInstanceSegments(const index_base::Version& version,
                                      const index_base::Version& lastVersion,
                                      const std::string& src);

    std::vector<VersionPair> GetValidVersions(const std::vector<index_base::Version>& versions,
                                              const index_base::Version& oldVersion);
    void CreateMergeSegmentForParallelInstance(
            const std::vector<VersionPair>& versionInfos,
            segmentid_t lastParentSegId, index_base::Version& newVersion);

    index_base::Version MakeNewVersion(const index_base::Version& lastVersion,
                                       const std::vector<VersionPair>& versionInfos,
                                       bool& hasNewSegment);

    void CollectSegmentDeletionFileInfo(const index_base::Version& version,
            index_base::DeletePatchInfos& segments, bool isSubPartition);

    void MergeDeletionMapFile(const std::vector<VersionPair>& versionInfos,
                              const file_system::DirectoryPtr& directory,
                              segmentid_t lastParentSegId);
    void DoMergeDeletionMapFile(const std::vector<VersionPair>& versionInfos,
                                const file_system::DirectoryPtr& directory,
                                segmentid_t lastParentSegId, bool isSubPartition);
    void MergeSegmentDeletionMapFile(
        segmentid_t segmentId, const file_system::DirectoryPtr& directory,
        const std::vector<index_base::PatchFileInfo>& files);
    void MergeCounterFile(const index_base::Version& newVersion,
                          const std::vector<VersionPair>& versionInfos,
                          const file_system::DirectoryPtr& directory,
                          segmentid_t lastParentSegId);
    void DoMergeCounterFile(const util::CounterMapPtr& counterMap,
                            const std::vector<VersionPair>& versionInfos);
    void UpdateStateCounter(const util::CounterMapPtr& counterMap,
                            const index_base::PartitionDataPtr& partData,
                            bool isSubPartition, bool needDeletionMap);
    util::StateCounterPtr GetStateCounter(const util::CounterMapPtr& counterMap,
                                            const std::string& counterName,
                                            bool isSubPartition);
    util::CounterMapPtr LoadCounterMap(const std::string& segmentPath);
    index_base::SegmentInfo CreateMergedSegmentInfo(const std::vector<VersionPair>& versionInfos);

    index_base::PartitionDataPtr CreateInstancePartitionData(
        const index_base::Version& version, bool isSubPartition);

    std::string GetParentPath(const std::vector<std::string>& srcPaths);

    bool IsEmptyVersions(const std::vector<index_base::Version>& versions,
                         const index_base::Version& lastVersion);

    // todo: remove
    versionid_t GetBaseVersionFromLagecyParallelBuild(
            const std::vector<index_base::Version>& versions);

private:
    file_system::DirectoryPtr mRoot;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
private:
    friend class ParallelPartitionDataMergerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ParallelPartitionDataMerger);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_PARALLEL_PARTITION_DATA_MERGER_H

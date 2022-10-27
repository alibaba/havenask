#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/offline_recover_strategy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/counter/state_counter.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/path_util.h"
#include <beeper/beeper.h>

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, ParallelPartitionDataMerger);

ParallelPartitionDataMerger::ParallelPartitionDataMerger(
        const string& mergePath,
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const IndexlibFileSystemPtr& fileSystem)
    : mSchema(schema)
    , mOptions(options)
{
    if (fileSystem)
    {
        mRoot = DirectoryCreator::Create(fileSystem, mergePath);
    }
    else
    {
        mRoot = DirectoryCreator::Create(mergePath);
    }
}

ParallelPartitionDataMerger::~ParallelPartitionDataMerger() 
{
}

Version ParallelPartitionDataMerger::MergeSegmentData(const vector<string>& srcPaths)
{
    IE_LOG(INFO, "Begin merge all data of parallel inc build into merge path.");
    Version lastVersion;
    VersionLoader::GetVersion(mRoot, lastVersion, INVALID_VERSION);
    IE_LOG(INFO, "lastest version [%d] in parent path : [%s]",
           lastVersion.GetVersionId(), lastVersion.ToString().c_str());
    string parentPath = GetParentPath(srcPaths);
    if (!FileSystemWrapper::IsExist(parentPath))
    {
        IE_LOG(WARN, "parallel path [%s] does not exist. "
               "maybe merge legacy non-parallel build index, no data to merge.",
               parentPath.c_str());
        return lastVersion;
    }
    
    versionid_t baseVersionId = GetBaseVersionIdFromParallelBuildInfo(srcPaths);
    auto versions = GetVersionList(srcPaths);
    if (baseVersionId == INVALID_VERSION)
    {
        if (lastVersion.GetVersionId() != INVALID_VERSION)
        {
            baseVersionId = GetBaseVersionFromLagecyParallelBuild(versions);
            if (baseVersionId == INVALID_VERSION)
            {
                // empty instance versions, use root last version
                baseVersionId = lastVersion.GetVersionId();
            }
        }
    }

    Version baseVersion;
    if (baseVersionId != INVALID_VERSION)
    {
        VersionLoader::GetVersion(mRoot, baseVersion, baseVersionId);
    }
    IE_LOG(INFO, "base version [%d] in parent path : [%s]",
           baseVersionId, baseVersion.ToString().c_str());
    
    if (IsEmptyVersions(versions, baseVersion))
    {
        // empty parent path & empty parallel inc path
        IE_LOG(INFO, "make empty version.");
        Version emptyVersion = versions[0];
        emptyVersion.SyncSchemaVersionId(mSchema);
        emptyVersion.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());
        
        VersionCommitter versionCommiter(mRoot, emptyVersion, INVALID_KEEP_VERSION_COUNT);
        versionCommiter.Commit();
        return emptyVersion;
    }
    
    auto versionInfos = GetValidVersions(versions, baseVersion);
    if (NoNeedMerge(versionInfos, lastVersion, baseVersionId))
    {
        IE_LOG(INFO, "merge work has done. Do nothing and exist directly");
        if (versions.size() > 0)
        {
            ProgressSynchronizer ps;
            ps.Init(versions);
            lastVersion.SetTimestamp(ps.GetTimestamp());
            lastVersion.SetFormatVersion(ps.GetFormatVersion());
        }
        return lastVersion;
    }
    for (auto &versionPair : versionInfos)
    {
        const Version& version = versionPair.first;
        const string& src = srcPaths[versionPair.second];
        MoveParallelInstanceSegments(version, baseVersion, src);
    }
    bool hasNewSegment = false;
    Version version = MakeNewVersion(baseVersion, versionInfos, hasNewSegment);
    if (srcPaths.size() > 1 && hasNewSegment && mSchema->GetTableType() != tt_customized)
    {
        CreateMergeSegmentForParallelInstance(
                versionInfos, baseVersion.GetLastSegment(), version);
    }

    version.SyncSchemaVersionId(mSchema);
    version.SetOngoingModifyOperations(mOptions.GetOngoingModifyOperationIds());    
    
    VersionCommitter versionCommiter(mRoot, version, INVALID_KEEP_VERSION_COUNT);
    versionCommiter.Commit();
    IE_LOG(INFO, "End Merged all data of parallel inc build into merge path.");
    return version;
}

void ParallelPartitionDataMerger::RemoveParallBuildDirectory()
{
    fslib::FileList fileList;
    mRoot->ListFile("", fileList);
    for (auto dir : fileList)
    {
        if (ParallelBuildInfo::IsParallelBuildPath(dir))
        {
            mRoot->RemoveDirectory(dir);
            IE_LOG(INFO, "remove parallel inc build path [%s].",
                   FileSystemWrapper::JoinPath(mRoot->GetPath(), dir).c_str());
        }
    }
}

versionid_t ParallelPartitionDataMerger::GetBaseVersionIdFromParallelBuildInfo(
        const vector<string>& srcPaths)
{
    if (srcPaths.empty())
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "number of parallel build instance path "
                             "must not be zero. partition path is [%s].",
                             mRoot->GetPath().c_str());
    }
    ParallelBuildInfo info;
    info.Load(srcPaths[0]);
    if (info.parallelNum != srcPaths.size())
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "number of parallel build instance path "
                             "must be same to parallNum in ParallelBuildInfo in path[%s]."
                             , srcPaths[0].c_str());
    }
    for (size_t i = 1; i < srcPaths.size(); i ++)
    {
        ParallelBuildInfo parallelInfo;
        parallelInfo.Load(srcPaths[i]);
        if (parallelInfo.parallelNum != info.parallelNum ||
            parallelInfo.batchId != info.batchId)
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "ParallelBuildInfo.parallelNum and"
                    "ParallelBuildInfo.batchId of each parallel build instance"
                    " must be same. [%s] and [%s] are different"
                    , ToJsonString(info).c_str(), ToJsonString(parallelInfo).c_str());
        }

        if (parallelInfo.GetBaseVersion() != info.GetBaseVersion())
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "ParallelBuildInfo.base_version of each parallel build instance"
                    " must be same. [%s] and [%s] are different"
                    , ToJsonString(info).c_str(), ToJsonString(parallelInfo).c_str());
        }
    }
    return info.GetBaseVersion();
}

vector<Version> ParallelPartitionDataMerger::GetVersionList(const vector<string>&srcPaths)
{
    vector<Version> versions;
    for (auto &src : srcPaths)
    {
        file_system::DirectoryPtr instanceRootDir = DirectoryCreator::Create(src);
        Version version;
        VersionLoader::GetVersion(src, version, INVALID_VERSION);

        Version recoverVersion = OfflineRecoverStrategy::Recover(version, instanceRootDir);
        Version diffVersion = recoverVersion - version;
        if (diffVersion.GetSegmentCount() > 0)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "[%s] has valid segment not in latest version!", src.c_str());
        }
        
        versions.push_back(version);
        if (version.GetVersionId() == INVALID_VERSION)
        {
            IE_LOG(INFO, "build parallel [%s] has no data.", src.c_str());
        }
    }
    return versions;
}

vector<ParallelPartitionDataMerger::VersionPair>
ParallelPartitionDataMerger::GetValidVersions(
        const vector<Version>& versions, const Version& baseVersion)
{
    vector<VersionPair> validVersions;
    set<segmentid_t> newSegments;
    for (size_t i = 0; i < versions.size(); i ++)
    {
        const Version& version = versions[i];
        if (version.GetVersionId() == INVALID_VERSION)
        {
            continue;
        }
        /**
        // if inc builder has built empty data, we still need to generate a new version
        if (version.GetSegmentVector() == baseVersion.GetSegmentVector())
        {
            IE_LOG(WARN, "segments in instance [%zu] version file just equal to parent version.", i);
            continue;
        }
        **/
        const Version::SegmentIdVec& buildSegments = version.GetSegmentVector();
        for (auto segment : buildSegments)
        {
            if (baseVersion.HasSegment(segment))
            {
                continue;
            }
            if (newSegments.find(segment) != newSegments.end())
            {
                INDEXLIB_FATAL_ERROR(InconsistentState, "parallel build segment must not be repeat. "
                        "repeat segment is segment_%d in parallel %lu", segment, i);
            }
            newSegments.insert(segment);
        }
        validVersions.emplace_back(version, i);
        IE_LOG(INFO, "found valid version [%d] in inc parallel %lu, version [%s]",
               version.GetVersionId(), i, version.ToString().c_str());
    }
    return validVersions;
}

bool ParallelPartitionDataMerger::NoNeedMerge(
        const vector<VersionPair>& versionInfos,
        const Version& lastVersion, versionid_t baseVersionId)
{
    if (versionInfos.empty())
    {
        return true;
    }

    if (lastVersion.GetVersionId() < baseVersionId)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "latestVersionId [%d] less than baseVersionId [%d]",
                             lastVersion.GetVersionId(), baseVersionId);
    }

    if (lastVersion.GetVersionId() == baseVersionId)
    {
        return false;
    }

    for (auto& versionInfo : versionInfos)
    {
        const Version::SegmentIdVec& buildSegments = versionInfo.first.GetSegmentVector();
        for (auto segment : buildSegments)
        {
            if (!lastVersion.HasSegment(segment))
            {
                IE_LOG(WARN, "segment [%d] not in version.%d, not a valid version, will be replaced!",
                       segment, lastVersion.GetVersionId());
                return false;
            }
        }
    }
    return true;
}

void ParallelPartitionDataMerger::MoveParallelInstanceSegments(
        const Version& version,
        const Version& baseVersion,
        const string& src)
{
    IE_LOG(INFO, "Begin MoveParallelInstanceSegments.");
    const Version::SegmentIdVec& buildSegments = version.GetSegmentVector();
    for (auto segment : buildSegments)
    {
        if (!baseVersion.HasSegment(segment))
        {
            string segmentDir = version.GetSegmentDirName(segment);
            string srcPath = FileSystemWrapper::JoinPath(src, segmentDir);
            string destPath = FileSystemWrapper::JoinPath(mRoot->GetPath(), segmentDir);
            if (FileSystemWrapper::IsExist(srcPath))
            {
                //remove destpath segment, which may be built by previous build.
                FileSystemWrapper::DeleteIfExist(destPath);
                FileSystemWrapper::Rename(srcPath, destPath);
                IE_LOG(INFO, "move parallel build segment of version.%d from"
                       " [%s] to [%s]", version.GetVersionId(), srcPath.c_str(), destPath.c_str());
            }
            else if (!FileSystemWrapper::IsExist(destPath))
            {
                INDEXLIB_FATAL_ERROR(SegmentFile, "parallel build segments"
                        "[%s] should have be moved into partition path[%s], but it was lost.",
                        srcPath.c_str(), destPath.c_str());
            }
        }
    }
    IE_LOG(INFO, "End MoveParallelInstanceSegments.");
}

Version ParallelPartitionDataMerger::MakeNewVersion(const Version& baseVersion,
                                                    const vector<VersionPair>& versionInfos,
                                                    bool& hasNewSegment)
{
    Version newVersion = baseVersion;
    vector<segmentid_t> segments;
    vector<Version> versions;
    for (auto &versionPair : versionInfos)
    {
        const Version& version = versionPair.first;
        versions.push_back(version);
        if (version.GetLastSegment() < baseVersion.GetLastSegment())
        {
            INDEXLIB_FATAL_ERROR(SegmentFile, "parallel build segment id[%d] should "
                    "be larger than segment[%d] in last merged version",
                    version.GetLastSegment(), baseVersion.GetLastSegment());
        }
        const Version::SegmentIdVec& buildSegments = version.GetSegmentVector();
        for (auto segment : buildSegments)
        {
            if (!baseVersion.HasSegment(segment))
            {
                segments.push_back(segment);
            }
        }
    }
    hasNewSegment = segments.size() > 0;
    sort(segments.begin(), segments.end());
    for (auto &segment: segments)
    {
        newVersion.AddSegment(segment);
    }

    ProgressSynchronizer ps;
    ps.Init(versions);
    newVersion.SetVersionId(baseVersion.GetVersionId() + 1);
    newVersion.SetTimestamp(ps.GetTimestamp());
    newVersion.SetLocator(ps.GetLocator());
    newVersion.SetFormatVersion(ps.GetFormatVersion());
    return newVersion;
}

void ParallelPartitionDataMerger::CreateMergeSegmentForParallelInstance(
        const vector<VersionPair>& versionInfos,
        segmentid_t lastParentSegId, Version& newVersion)
{
    IE_LOG(INFO, "Begin create merged segment for BEGIN MERGE");
    segmentid_t mergeNewSegment = newVersion.GetLastSegment() + 1;
    Version version = newVersion;
    newVersion.AddSegment(mergeNewSegment);
    string segmentDir = newVersion.GetSegmentDirName(mergeNewSegment);
    if (mRoot->IsExist(segmentDir))
    {
        mRoot->RemoveDirectory(segmentDir);   
    }
    DirectoryPtr directory = mRoot->MakeDirectory(segmentDir);
    if (mSchema->GetTableType() == tt_index)
    {
        MergeDeletionMapFile(versionInfos, directory, lastParentSegId);
    }

    SegmentInfo segmentInfo = CreateMergedSegmentInfo(versionInfos);
    segmentInfo.Store(directory);
    if (mSchema->GetSubIndexPartitionSchema())
    {
        // make sub segment directory if not exist.
        DirectoryPtr subDirectory = directory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        segmentInfo.Store(subDirectory);
    }
    // write counter after segment info, to load last segment deletionmap
    MergeCounterFile(newVersion, versionInfos, directory, lastParentSegId);
    // write deploy_index file after counter, to write counter info into deploy_index
    DeployIndexWrapper::DumpSegmentDeployIndex(directory);
    IE_LOG(INFO, "End Create merged segment [%d] of parallel build index",
           mergeNewSegment);
    BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
            "Create merged segment [%d] for parallel build index, segment info : %s",
            mergeNewSegment, segmentInfo.ToString(true).c_str());
}

void ParallelPartitionDataMerger::MergeDeletionMapFile(
        const vector<VersionPair>& versionInfos,
        const DirectoryPtr& directory,
        segmentid_t lastParentSegId)
{
    DoMergeDeletionMapFile(versionInfos, directory, lastParentSegId, false);
    if (mSchema->GetSubIndexPartitionSchema() != NULL)
    {
        DirectoryPtr subDirectory = directory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        DoMergeDeletionMapFile(versionInfos, subDirectory, lastParentSegId, true);
    }
}

void ParallelPartitionDataMerger::DoMergeDeletionMapFile(
        const vector<VersionPair>& versionInfos, const DirectoryPtr& directory,
        segmentid_t lastParentSegId, bool isSubPartition)
{
    IE_LOG(INFO, "Begin Merge deletion map files in [%s]", directory->GetPath().c_str());
    file_system::DirectoryPtr delMapDirectory =
            directory->MakeDirectory(DELETION_MAP_DIR_NAME);
    map<segmentid_t, vector<PatchFileInfo>> toDeleteSegments;
    for (auto &versionPair : versionInfos)
    {
        DeletePatchInfos segments;
        CollectSegmentDeletionFileInfo(versionPair.first, segments, isSubPartition);
        for (auto &p : segments)
        {
            if (p.first > lastParentSegId)
            {
                continue;
            }
            toDeleteSegments[p.first].push_back(p.second);
        }
    }
    for (auto &infos : toDeleteSegments)
    {
        MergeSegmentDeletionMapFile(infos.first, delMapDirectory, infos.second);
    }
    IE_LOG(INFO, "End Merge deletion map files in [%s]", directory->GetPath().c_str());
}

void ParallelPartitionDataMerger::MergeSegmentDeletionMapFile(
        segmentid_t segmentId,
        const DirectoryPtr& directory,
        const vector<index_base::PatchFileInfo>& files)
{
    if (files.size() == 1)
    {
        return;
    }
    DeletionMapSegmentWriter segmentWriter;
    segmentWriter.Init(files[0].patchDirectory,
                       files[0].patchFileName, false);
    for (size_t i = 1; i < files.size(); i++)
    {
        auto fileReader = files[i].patchDirectory->CreateFileReader(
                files[i].patchFileName, file_system::FSOT_IN_MEM);
        segmentWriter.MergeDeletionMapFile(fileReader);
    }
    segmentWriter.Dump(directory, segmentId, true);
}


void ParallelPartitionDataMerger::MergeCounterFile(
        const Version& newVersion,
        const vector<VersionPair>& versionInfos,
        const DirectoryPtr& directory,
        segmentid_t lastParentSegId)
{
    const Version::SegmentIdVec& segIdVec = newVersion.GetSegmentVector();

    // segment(lastParentSegId) will not exist for recovered index
    auto GetValidParentLastSegmentId = [segIdVec](segmentid_t lastParentSegId) {
        int cursor = -1;
        for (auto segId : segIdVec)
        {
            if (segId > lastParentSegId)
            {
                break;
            }
            cursor++;
        }
        return cursor >= 0 ? segIdVec[cursor] : INVALID_SEGMENTID;
    };

    segmentid_t lastValidParentSegId = GetValidParentLastSegmentId(lastParentSegId);
    CounterMapPtr lastCounterMap(new CounterMap);
    if (lastValidParentSegId != INVALID_SEGMENTID)
    {
        std::string lastParentSegPath = newVersion.GetSegmentDirName(lastValidParentSegId);
        lastCounterMap = LoadCounterMap(lastParentSegPath);
    }
    DoMergeCounterFile(lastCounterMap, versionInfos);
    auto partData = CreateInstancePartitionData(newVersion, false);
    UpdateStateCounter(lastCounterMap, partData, false,
                       mSchema->GetTableType() == tt_index);

    if (mSchema->GetSubIndexPartitionSchema() != NULL)
    {
        DirectoryPtr subDirectory = directory->MakeDirectory(SUB_SEGMENT_DIR_NAME);
        UpdateStateCounter(lastCounterMap, partData->GetSubPartitionData(), true,
                           mSchema->GetTableType() == tt_index);
        subDirectory->Store(COUNTER_FILE_NAME, lastCounterMap->ToJsonString(), true);
    }
    directory->Store(COUNTER_FILE_NAME, lastCounterMap->ToJsonString(), true);
}

void ParallelPartitionDataMerger::DoMergeCounterFile(
        const CounterMapPtr& lastCounterMap,
        const std::vector<VersionPair>& versionInfos)
{
    for (auto &versionPair : versionInfos)
    {
        const Version& version = versionPair.first;
        if (version.GetSegmentCount() == 0)
        {
            continue;
        }
        string segDir = version.GetSegmentDirName(version.GetSegmentVector().back());
        CounterMapPtr counterMap = LoadCounterMap(segDir);
        lastCounterMap->Merge(counterMap->ToJsonString(false), CounterBase::FJT_MERGE);
    }
}

void ParallelPartitionDataMerger::UpdateStateCounter(
        const CounterMapPtr& counterMap,
        const PartitionDataPtr& partData,
        bool isSubPartition, bool needDeletionMap)
{
    assert(partData);
    auto partitionDocCounter = GetStateCounter(counterMap, "partitionDocCount", isSubPartition);
    auto deletedDocCounter = GetStateCounter(counterMap, "deletedDocCount", isSubPartition);

    const SegmentDataVector& segmentDatas = partData->GetSegmentDirectory()->GetSegmentDatas();
    SegmentDataVector::const_reverse_iterator it = segmentDatas.rbegin();
    int64_t docCount = 0;
    if (it != segmentDatas.rend())
    {
        docCount = it->GetBaseDocId() + it->GetSegmentInfo().docCount;
    }
    partitionDocCounter->Set(docCount);
    uint32_t delDocCount = 0;
    if (needDeletionMap)
    {
        DeletionMapReaderPtr deletionMapReader(new DeletionMapReader);
        deletionMapReader->Open(partData.get());
        delDocCount = deletionMapReader->GetDeletedDocCount();
    }
    deletedDocCounter->Set(delDocCount);

    if (isSubPartition)
    {
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "sub table [%s]: partitionDocCount [%ld], deletedDocCount [%u]",
                mSchema->GetSchemaName().c_str(), docCount, delDocCount);
    }
    else
    {
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "table [%s]: partitionDocCount [%ld], deletedDocCount [%u]",
                mSchema->GetSchemaName().c_str(), docCount, delDocCount);
    }
}

StateCounterPtr ParallelPartitionDataMerger::GetStateCounter(
        const CounterMapPtr& counterMap,
        const string& counterName,
        bool isSub)
{
    string prefix = isSub ? "offline.sub_" : "offline.";
    string counterPath = prefix + counterName;
    auto stateCounter = counterMap->GetStateCounter(counterPath);
    if (!stateCounter)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterPath.c_str());
    }
    return stateCounter;
}

CounterMapPtr ParallelPartitionDataMerger::LoadCounterMap(
        const string& segmentDir)
{
    DirectoryPtr segDir = mRoot->GetDirectory(segmentDir, true);
    string counterString;
    segDir->Load(COUNTER_FILE_NAME, counterString, true);
    CounterMapPtr counterMap(new CounterMap);
    counterMap->FromJsonString(counterString);
    return counterMap;
}

void ParallelPartitionDataMerger::CollectSegmentDeletionFileInfo(
        const Version& version, DeletePatchInfos& segments,
        bool isSubPartition)
{
    PartitionDataPtr partData =
        CreateInstancePartitionData(version, isSubPartition);
    PatchFileFinderPtr patchFinder =
        PatchFileFinderCreator::Create(partData.get());
    patchFinder->FindDeletionMapFiles(segments);
}

SegmentInfo ParallelPartitionDataMerger::CreateMergedSegmentInfo(
        const vector<VersionPair>& versionInfos)
{
    vector<SegmentInfo> segInfos;
    uint32_t maxTTL = 0;
    // set max TTL
    for (auto vp : versionInfos)
    {
        const Version& version = vp.first;
        if (version.GetSegmentCount() == 0)
        {
            continue;
        }
        string segDir = version.GetSegmentDirName(version.GetSegmentVector().back());
        DirectoryPtr segmentDirectory = mRoot->GetDirectory(segDir, true);
        SegmentInfo info;
        if (!info.Load(segmentDirectory))
        {
            INDEXLIB_FATAL_ERROR(InconsistentState, "segment [%d] has no segment info",
                    version.GetSegmentVector().back());
        }
        maxTTL = max(maxTTL, info.maxTTL);
        segInfos.push_back(info);
    }

    SegmentInfo segInfo;
    segInfo.maxTTL = maxTTL;
    
    // set ts & locator
    ProgressSynchronizer ps;
    ps.Init(segInfos);
    segInfo.timestamp = ps.GetTimestamp();
    segInfo.locator = ps.GetLocator();
    segInfo.schemaId = mSchema->GetSchemaVersionId();
    return segInfo;
}

PartitionDataPtr ParallelPartitionDataMerger::CreateInstancePartitionData(
        const Version& version, bool isSubPartition)
{
    IndexlibFileSystemPtr fileSystem = mRoot->GetFileSystem();
    PartitionDataPtr partData = merger::MergePartitionDataCreator::CreateMergePartitionData(
            fileSystem, mSchema, version, "");
    return isSubPartition ? partData->GetSubPartitionData() : partData;
}

vector<string> ParallelPartitionDataMerger::GetParallelInstancePaths(
        const string& rootDir, const ParallelBuildInfo& parallelInfo)
{
    vector<string> mergeSrc;
    for (size_t i = 0; i < parallelInfo.parallelNum; i ++)
    {
        ParallelBuildInfo info = parallelInfo;
        info.instanceId = i;
        mergeSrc.push_back(info.GetParallelInstancePath(rootDir));
    }
    return mergeSrc;
}

string ParallelPartitionDataMerger::GetParentPath(const vector<string>& srcPaths)
{
    if (srcPaths.empty())
    {
        INDEXLIB_FATAL_ERROR(BadParameter, "number of parallel build instance path "
                             "must not be zero. partition path is [%s].",
                             mRoot->GetPath().c_str());
    }
    string parentPath = PathUtil::GetParentDirPath(srcPaths[0]);
    for (size_t i = 1; i < srcPaths.size(); i++)
    {
        string tmpParentPath = PathUtil::GetParentDirPath(srcPaths[i]);
        if (parentPath != tmpParentPath)
        {
            INDEXLIB_FATAL_ERROR(BadParameter,
                    "parallel instance path [%s] does not has same parent path with [%s].",
                    srcPaths[i].c_str(), srcPaths[0].c_str());
        }
    }
    return parentPath;
}

bool ParallelPartitionDataMerger::IsEmptyVersions(
        const vector<Version>& versions, const Version& lastVersion)
{
    if (lastVersion.GetVersionId() != INVALID_VERSION)
    {
        return false;
    }

    for (auto version : versions)
    {
        if (!version.GetSegmentVector().empty())
        {
            return false;
        }
    }
    return true;
}

// todo : remove legacy code
versionid_t ParallelPartitionDataMerger::GetBaseVersionFromLagecyParallelBuild(
        const vector<Version>& versions)
{
    versionid_t vid = INVALID_VERSION;
    for (auto &version : versions)
    {
        if (version.GetVersionId() == INVALID_VERSION)
        {
            continue;
        }

        if (vid == INVALID_VERSION)
        {
            vid = version.GetVersionId();
        }
        else if (vid != version.GetVersionId())
        {
            INDEXLIB_FATAL_ERROR(BadParameter, "legacy parallel build version must be same. "
                    "version.%u and version.%u  are different",
                    version.GetVersionId(), vid);
        }
    }
    if (vid != INVALID_VERSION)
    {
        return (vid - 1);
    }
    return INVALID_VERSION;
}

IE_NAMESPACE_END(merger);


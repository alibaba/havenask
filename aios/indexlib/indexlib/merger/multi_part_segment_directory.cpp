#include <sstream>
#include <autil/StringUtil.h>
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/segment_directory_finder.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/merger/multi_part_counter_merger.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/counter/counter_base.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/index_meta/progress_synchronizer.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MultiPartSegmentDirectory);

MultiPartSegmentDirectory::MultiPartSegmentDirectory(
        const vector<string>& roots,
        const vector<Version>& versions)
    : SegmentDirectory()
{
    assert(roots.size() == versions.size());
    mRootDirs = roots;
    mVersions = versions;
    mPartSegIds.resize(versions.size());
    for (size_t i = 0; i < mRootDirs.size(); ++i)
    {
        mRootDirs[i] = FileSystemWrapper::NormalizeDir(mRootDirs[i]);
    }
}

MultiPartSegmentDirectory::~MultiPartSegmentDirectory()
{
}

void MultiPartSegmentDirectory::DoInit(bool hasSub, bool needDeletionMap,
                                       const IndexlibFileSystemPtr& fileSystem)
{
    segmentid_t virtualSegId = 0;
    mVersion = Version(0);
    for (auto &version : mVersions)
    {
        if (version.GetVersionId() == INVALID_VERSION)
        {
            continue;
        }
        const LevelInfo& srcLevelInfo = version.GetLevelInfo();
        if (!srcLevelInfo.levelMetas.empty())
        {
            LevelInfo& levelInfo = mVersion.GetLevelInfo();
            levelInfo.Init(srcLevelInfo.GetTopology(), srcLevelInfo.GetLevelCount(),
                           srcLevelInfo.GetColumnCount());
        }
        break;
    }

    for (size_t i = 0; i < mVersions.size(); ++i)
    {
        for (size_t j = 0; j < mVersions[i].GetSegmentCount(); ++j)
        {
            InitOneSegment(i, mVersions[i][j], virtualSegId, mVersions[i]);
            mVersion.AddSegment(virtualSegId++);
        }
    }

    if (!mVersions.empty())
    {
        ProgressSynchronizer ps;
        ps.Init(mVersions);
        mVersion.SetTimestamp(ps.GetTimestamp());
        mVersion.SetLocator(ps.GetLocator());
        mVersion.SetFormatVersion(ps.GetFormatVersion());
    }
    mPartitionData = merger::MergePartitionDataCreator::CreateMergePartitionData(
            mRootDirs, mVersions, hasSub);
    if (needDeletionMap)
    {
        mDeletionMapReader.reset(new DeletionMapReader);
        mDeletionMapReader->Open(mPartitionData.get());
    }
}

void MultiPartSegmentDirectory::InitOneSegment(partitionid_t partId,
        segmentid_t physicalSegId, segmentid_t virtualSegId, const Version& version)
{
    assert(partId < (partitionid_t)mPartSegIds.size());
    string segPath = SegmentDirectoryFinder::MakeSegmentPath(mRootDirs[partId],
            physicalSegId, version);
    Segment segment(segPath, partId, physicalSegId);
    mSegments[virtualSegId] = segment;
    mPartSegIds[partId].push_back(virtualSegId);
}

SegmentDirectory* MultiPartSegmentDirectory::Clone() const
{
    return new MultiPartSegmentDirectory(*this);
}

segmentid_t MultiPartSegmentDirectory::GetVirtualSegmentId(
        segmentid_t virtualSegIdInSamePartition,
        segmentid_t physicalSegId) const
{
    Segments::const_iterator iter = mSegments.find(virtualSegIdInSamePartition);
    assert(iter != mSegments.end());
    partitionid_t partId = iter->second.partId;

    for (Segments::const_iterator it = mSegments.begin();
         it != mSegments.end(); ++it)
    {
        if (it->second.physicalSegId == physicalSegId
            && it->second.partId == partId)
        {
            return it->first;
        }
    }
    return INVALID_SEGMENTID;
}

void MultiPartSegmentDirectory::GetPhysicalSegmentId(segmentid_t virtualSegId,
            segmentid_t& physicalSegId, partitionid_t& partId) const
{
    Segments::const_iterator iter = mSegments.find(virtualSegId);
    assert(iter != mSegments.end());
    partId = iter->second.partId;
    physicalSegId = iter->second.physicalSegId;
}

void MultiPartSegmentDirectory::Reload(const vector<Version>& versions)
{
    assert(versions.size() == mVersions.size());
    bool hasSub = mSubSegmentDirectory;
    bool needDeletionMap = GetDeletionMapReader();
    mVersions = versions;
    SegmentDirectory::Reset();
    for (size_t i = 0; i < mPartSegIds.size(); ++i)
    {
        mPartSegIds[i].clear();
    }
    Init(hasSub, needDeletionMap);
}

string MultiPartSegmentDirectory::GetLatestCounterMapContent() const
{
    if (mPartSegIds.empty())
    {
        return CounterMap::EMPTY_COUNTER_MAP_JSON;
    }

    bool hasSub = mSubSegmentDirectory;
    MultiPartCounterMerger counterMerger(hasSub);
    for (const auto& segVec : mPartSegIds)
    {
        if (segVec.empty())
        {
            continue;
        }
        segmentid_t lastSegIdOfPartition = segVec.back();
        counterMerger.Merge(GetCounterMapContent(mSegments.at(lastSegIdOfPartition)));
    }
    return counterMerger.ToJsonString();
}

IE_NAMESPACE_END(merger);


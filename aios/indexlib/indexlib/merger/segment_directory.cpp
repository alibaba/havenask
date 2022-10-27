#include <sstream>
#include <autil/StringUtil.h>
#include "indexlib/merger/segment_directory.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/merger/segment_directory_finder.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/index_base/patch/partition_patch_meta.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, SegmentDirectory);

//TODO: remove?
SegmentDirectory::SegmentDirectory()
{
    mSegInfoInited = false;
    mTotalDocCount = 0;
}

SegmentDirectory::SegmentDirectory(const string& root,
                                   const Version& version)
    : SegmentDirectoryBase(version)
    , mRootDir(FileSystemWrapper::NormalizeDir(root))
    , mNewVersion(version)
{
    assert(!mRootDir.empty());

    mSegInfoInited = false;
    mTotalDocCount = 0;
    InitSegIdToSegmentMap();
}

SegmentDirectory::~SegmentDirectory() 
{
}

void SegmentDirectory::Init(bool hasSub, bool needDeletionMap,
                            const IndexlibFileSystemPtr& fileSystem)
{
    DoInit(hasSub, needDeletionMap, fileSystem);
    if (hasSub)
    {
        mSubSegmentDirectory = CreateSubSegDir(needDeletionMap);
    }
}

void SegmentDirectory::DoInit(bool hasSub, bool needDeletionMap,
                              const IndexlibFileSystemPtr& fileSystem)
{
    InitSegIdToSegmentMap();
    InitPartitionData(hasSub, needDeletionMap, fileSystem);
}

string SegmentDirectory::GetSegmentPath(segmentid_t segId) const
{
    Segments::const_iterator it = mSegments.find(segId);
    if (it != mSegments.end())
    {
        return it->second.segPath;
    }
    return "";
}

string SegmentDirectory::GetRootDir() const
{
    return mRootDir;
}

string SegmentDirectory::GetFirstValidRootDir() const
{
    return mRootDir;
}

void SegmentDirectory::ListAttrPatch(const string& attrName, segmentid_t segId,
                                     vector<pair<string, segmentid_t> >& patches) const
{
    SegmentDirectoryFinder::ListAttrPatch(this, attrName, segId, patches);
}
      
void SegmentDirectory::ListAttrPatch(const string& attrName, segmentid_t segId, 
                                     vector<string>& patches) const
{
    SegmentDirectoryFinder::ListAttrPatch(this, attrName, segId, patches);
}
   
void SegmentDirectory::InitSegmentInfo()
{
    if (mSegInfoInited) 
    {
        return;
    }

    assert(mVersion.GetSegmentCount() == mSegments.size());

    mTotalDocCount = 0;
    Segments::const_iterator it = mSegments.begin();
    for(; it != mSegments.end(); ++it)
    {
        SegmentInfo segInfo;
        string segmentPath = (it->second).segPath;
        if (!LoadSegmentInfo(segmentPath, segInfo))
        {
            string segmentMetaPath = segmentPath + SEGMENT_INFO_FILE_NAME;
            INDEXLIB_FATAL_ERROR(FileIO, "Load segment meta file failed, "
                    "file path : %s", segmentMetaPath.c_str());
        }
        segmentid_t segId = it->first;
        mSegmentInfos[segId] = segInfo;
        mBaseDocIds.push_back(mTotalDocCount);
        mTotalDocCount += segInfo.docCount;
    }
    mSegInfoInited = true;
}

void SegmentDirectory::InitSegIdToSegmentMap()
{
    Version::Iterator iter = mVersion.CreateIterator();
    while (iter.HasNext())
    {
        segmentid_t segId = iter.Next();
        string segDirName = mVersion.GetSegmentDirName(segId);
        string segPath = FileSystemWrapper::JoinPath(mRootDir, segDirName) + "/";
        Segment segment(segPath, 0, segId);
        mSegments.insert(std::make_pair(segId, segment));
    }
}

void SegmentDirectory::GetBaseDocIds(std::vector<exdocid_t>& baseDocIds)
{
    if (!mSegInfoInited)
    {
        InitSegmentInfo();
    }

    baseDocIds = mBaseDocIds;
}

uint64_t SegmentDirectory::GetTotalDocCount()
{
    if (!mSegInfoInited)
    {
        InitSegmentInfo();
    }
    return mTotalDocCount;
}

bool SegmentDirectory::GetSegmentInfo(segmentid_t segId, SegmentInfo& segInfo)
{
    if (!mSegInfoInited)
    {
        InitSegmentInfo();
    }
    SegmentInfoMap::const_iterator it = mSegmentInfos.find(segId);
    if (it == mSegmentInfos.end())
    {
        return false;
    }
    segInfo = it->second;
    return true;
}

uint32_t SegmentDirectory::GetDocCount(segmentid_t segId)
{
    if (!mSegInfoInited)
    {
        InitSegmentInfo();
    }
    SegmentInfoMap::const_iterator it = mSegmentInfos.find(segId);
    if (it == mSegmentInfos.end())
    {
        return 0;
    }
    return it->second.docCount;
}

bool SegmentDirectory::LoadSegmentInfo(
        const string& segPath, SegmentInfo &segInfo)
{
    string segFileName = SEGMENT_INFO_FILE_NAME;
    return segInfo.Load(FileSystemWrapper::JoinPath(segPath, segFileName));
}

SegmentDirectoryPtr SegmentDirectory::CreateSubSegDir(bool needDeletionMap) const
{
    SegmentDirectoryPtr segDir(Clone());
    Segments subSegments;
    const Segments& mainSegments = segDir->GetSegments();
    Segments::const_iterator it = mainSegments.begin();
    for (; it != mainSegments.end(); ++it)
    {
        Segment subSegment = it->second;
        subSegment.segPath = 
            FileSystemWrapper::NormalizeDir(FileSystemWrapper::JoinPath(
                            subSegment.segPath, SUB_SEGMENT_DIR_NAME));
        subSegments.insert(std::make_pair(it->first, subSegment));
    }
    segDir->SetSegments(subSegments);
    //TODO:
    if (mPartitionData)
    {
        segDir->SetPartitionData(mPartitionData->GetSubPartitionData());
        if (needDeletionMap)
        {
            segDir->mDeletionMapReader.reset(new DeletionMapReader);
            segDir->mDeletionMapReader->Open(mPartitionData->GetSubPartitionData().get());
        }
    }
    return segDir;
}

SegmentDirectory* SegmentDirectory::Clone() const
{
    return new SegmentDirectory(*this);
}

segmentid_t SegmentDirectory::GetVirtualSegmentId(segmentid_t virtualSegIdInSamePartition,
        segmentid_t physicalSegId) const
{
    return physicalSegId;
}

bool SegmentDirectory::IsExist(const std::string& filePath) const
{
    return FileSystemWrapper::IsExist(filePath);
}

void SegmentDirectory::InitPartitionData(bool hasSub, bool needDeletionMap,
                                         const IndexlibFileSystemPtr& fileSystem)
{
    if (!fileSystem)
    {
        IndexlibFileSystemPtr newFileSystem(new file_system::IndexlibFileSystemImpl(mRootDir));
        file_system::FileSystemOptions options;
        options.useCache = false;
        options.memoryQuotaController =
            MemoryQuotaControllerCreator::CreatePartitionMemoryController();
        options.enablePathMetaContainer = true;
        newFileSystem->Init(options);
        mPartitionData = merger::MergePartitionDataCreator::CreateMergePartitionData(
            newFileSystem, mVersion, "", hasSub);
    }
    else
    {
        mPartitionData = merger::MergePartitionDataCreator::CreateMergePartitionData(
            fileSystem, mVersion, "", hasSub);
    }
    if (needDeletionMap)
    {
        mDeletionMapReader.reset(new DeletionMapReader);
        mDeletionMapReader->Open(mPartitionData.get());
    }
}

index::DeletionMapReaderPtr SegmentDirectory::GetDeletionMapReader() const
{
    return mDeletionMapReader;
}

void SegmentDirectory::RemoveSegment(segmentid_t segmentId)
{
    mNewVersion.RemoveSegment(segmentId);
    if (mNewVersion.GetVersionId() == mVersion.GetVersionId())
    {
        mNewVersion.IncVersionId();        
    }
}

file_system::DirectoryPtr SegmentDirectory::CreateNewSegment(SegmentInfo& segInfo)
{
    file_system::DirectoryPtr segDir;
    segmentid_t newSegmentId = 0;
    segmentid_t lastSegId = mVersion.GetLastSegment();
    if (lastSegId != INVALID_SEGMENTID)
    {
        segInfo.mergedSegment = false;
        segInfo.docCount = 0;
        segInfo.maxTTL = 0;

        SegmentInfo lastSegInfo;
        if (GetSegmentInfo(lastSegId, lastSegInfo))
        {
            segInfo.locator = lastSegInfo.locator;
            segInfo.timestamp = lastSegInfo.timestamp;
        }
        else
        {
            segInfo.locator = mVersion.GetLocator();
            segInfo.timestamp = mVersion.GetTimestamp();
        }
        newSegmentId = lastSegId + 1;
    }
    string segDirName = mVersion.GetNewSegmentDirName(newSegmentId);
    if (!mPartitionData)
    {
        return segDir;
    }
    file_system::DirectoryPtr partitionDir = mPartitionData->GetRootDirectory();
    if (!partitionDir)
    {
        return segDir;
    }
    segDir = partitionDir->MakeDirectory(segDirName);
    if (!segDir)
    {
        return segDir;
    }
    if (mSubSegmentDirectory)
    {
        segDir->MakeDirectory(SUB_SEGMENT_DIR_NAME);
    }
    mNewVersion = mVersion;
    mNewVersion.AddSegment(newSegmentId);
    mNewVersion.IncVersionId();
    return segDir;
}

void SegmentDirectory::CommitVersion()
{
    if (mNewVersion == mVersion)
    {
        return;
    }

    VersionCommitter::DumpPartitionPatchMeta(GetRootDir(), mNewVersion);
    DeployIndexWrapper::DumpDeployMeta(GetRootDir(), mNewVersion);
    mNewVersion.Store(GetRootDir(), false);
    IndexSummary ret = IndexSummary::Load(GetRootDir(),
            mNewVersion.GetVersionId(), mVersion.GetVersionId());
    ret.Commit(GetRootDir());
    mVersion = mNewVersion;
    Reload();
}

void SegmentDirectory::Reload()
{
    bool hasSub = mSubSegmentDirectory;
    bool needDeletionMap = GetDeletionMapReader();
    Reset();
    Init(hasSub, needDeletionMap);
}

void SegmentDirectory::Reset()
{
    mNewVersion = mVersion;
    mSegments.clear();
    mSegmentInfos.clear();
    mBaseDocIds.clear();
    mTotalDocCount = 0;
    mSegInfoInited = false;
    mPartitionData.reset();
    mDeletionMapReader.reset();
    mSubSegmentDirectory.reset();
}

void SegmentDirectory::RemoveUselessSegment(const string& rootDir)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    segmentid_t lastSegmentId = version.GetLastSegment();
    fslib::FileList segmentList;
    VersionLoader::ListSegments(rootDir, segmentList);
    for (size_t i = 0; i < segmentList.size(); ++i)
    {
        if (Version::GetSegmentIdByDirName(segmentList[i]) > lastSegmentId)
        {
            FileSystemWrapper::DeleteDir(FileSystemWrapper::JoinPath(
                            rootDir, segmentList[i]));
        }
    }
}

string SegmentDirectory::GetCounterMapContent(const Segment& segment) const
{
    string segmentPath = segment.segPath;
    string counterFilePath = FileSystemWrapper::JoinPath(segmentPath, COUNTER_FILE_NAME);
    string counterMapContent;
    try
    {
        if (!FileSystemWrapper::AtomicLoad(counterFilePath, counterMapContent, true))
        {
            IE_LOG(WARN, "no counter file found in [%s]", segment.segPath.c_str());
            return CounterMap::EMPTY_COUNTER_MAP_JSON;
        }
    }
    catch (...)
    {
        throw;
    }
    return counterMapContent;    
}

string SegmentDirectory::GetLatestCounterMapContent() const
{
    if (mSegments.empty())
    {
        return CounterMap::EMPTY_COUNTER_MAP_JSON;
    }
    return GetCounterMapContent(mSegments.rbegin()->second);
}

vector<segmentid_t> SegmentDirectory::GetNewSegmentIds() const
{
    Version diff = mNewVersion - mVersion;
    return diff.GetSegmentVector();
}

IE_NAMESPACE_END(merger);


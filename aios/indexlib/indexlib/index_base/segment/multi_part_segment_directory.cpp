#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/util/path_util.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MultiPartSegmentDirectory);

MultiPartSegmentDirectory::MultiPartSegmentDirectory() 
{
}

MultiPartSegmentDirectory::MultiPartSegmentDirectory(
        const MultiPartSegmentDirectory& other)
    : SegmentDirectory(other)
    , mEndSegmentIds(other.mEndSegmentIds)
{
    mSegmentDirectoryVec.clear();
    for (size_t i = 0; i < other.mSegmentDirectoryVec.size(); ++i)
    {
        SegmentDirectoryPtr segDir(other.mSegmentDirectoryVec[i]->Clone());
        mSegmentDirectoryVec.push_back(segDir);
    }
}

MultiPartSegmentDirectory::~MultiPartSegmentDirectory() 
{
}

SegmentDirectory* MultiPartSegmentDirectory::Clone()
{
    return new MultiPartSegmentDirectory(*this);
}

void MultiPartSegmentDirectory::Init(const file_system::DirectoryVector& directoryVec,
                                     bool hasSub)
{
    vector<Version> versions;
    for (size_t i = 0; i < directoryVec.size(); ++i)
    {
        Version version;
        VersionLoader::GetVersion(directoryVec[i], version, INVALID_VERSION);
        versions.push_back(version);
    }
    Init(directoryVec, versions, hasSub);
}

void MultiPartSegmentDirectory::Init(const file_system::DirectoryVector& directoryVec,
                                     const std::vector<index_base::Version>& versions,
                                     bool hasSub)
{
    assert(directoryVec.size() == versions.size());
    segmentid_t virtualSegId = 0;
    for (size_t i = 0; i < directoryVec.size(); ++i)
    {
        SegmentDirectoryPtr segDir(new SegmentDirectory);
        segDir->Init(directoryVec[i], versions[i]);
        mSegmentDirectoryVec.push_back(segDir);

        Version version = segDir->GetVersion();
        for (size_t i = 0; i < version.GetSegmentCount(); ++i)
        {
            mVersion.AddSegment(virtualSegId++);
        }
        mEndSegmentIds.push_back(virtualSegId);
    }

    InitSegmentDatas(mVersion);
    if (!versions.empty())
    {
        mVersion.SetTimestamp(versions.rbegin()->GetTimestamp());
        mVersion.SetLocator(versions.rbegin()->GetLocator());
    }
    mOnDiskVersion = mVersion;
    
    if (mSegmentDirectoryVec.size() > 0)
    {
        //TODO: use first seg dir to init global member var
        SegmentDirectoryPtr segDir = mSegmentDirectoryVec[0];
        mRootDirectory = segDir->GetRootDirectory();
        mRootDir = mRootDirectory->GetPath();
    }
    if (hasSub)
    {
        mSubSegmentDirectory.reset(Clone());
        mSubSegmentDirectory->SetSubSegmentDir();
    }
}

file_system::DirectoryPtr MultiPartSegmentDirectory::GetSegmentParentDirectory(
        segmentid_t segId) const
{
    SegmentDirectoryPtr segDir = GetSegmentDirectory(segId);
    assert(segDir);
    return segDir->GetRootDirectory();
}

segmentid_t MultiPartSegmentDirectory::EncodeToVirtualSegmentId(
        size_t partitionIdx, segmentid_t phySegId) const
{
    if (partitionIdx >= mEndSegmentIds.size())
    {
        return INVALID_SEGMENTID;
    }

    segmentid_t startSegmentId = 0;
    if (partitionIdx != 0)
    {
        startSegmentId = mEndSegmentIds[partitionIdx - 1];
    }

    const Version& version = 
        mSegmentDirectoryVec[partitionIdx]->GetVersion();
    for (size_t i = 0; i < version.GetSegmentCount(); i++)
    {
        segmentid_t segId = version[i];
        if (phySegId == segId)
        {
            return startSegmentId + (segmentid_t)i;
        }
    }
    return INVALID_SEGMENTID;
}

bool MultiPartSegmentDirectory::DecodeVirtualSegmentId(segmentid_t virtualSegId,
        segmentid_t &physicalSegId,
        size_t &partitionId) const
{
    assert(mEndSegmentIds.size() == mSegmentDirectoryVec.size());
    SegmentIdVector::const_iterator it = std::upper_bound(mEndSegmentIds.begin(),
            mEndSegmentIds.end(), virtualSegId);
    if (it == mEndSegmentIds.end())
    {
        return false;
    }

    partitionId = it - mEndSegmentIds.begin();
    Version version = mSegmentDirectoryVec[partitionId]->GetVersion();
    size_t idx = virtualSegId;
    if (partitionId > 0)
    {
        idx = virtualSegId - mEndSegmentIds[partitionId - 1];
    }
    assert(idx < version.GetSegmentCount());
    physicalSegId = version[idx];
    return true;
}

SegmentDirectoryPtr MultiPartSegmentDirectory::GetSegmentDirectory(
        segmentid_t segId) const
{
    size_t partitionId = 0;
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    if (!DecodeVirtualSegmentId(segId, physicalSegId, partitionId))
    {
        return SegmentDirectoryPtr();
    }
    assert(partitionId < mSegmentDirectoryVec.size());
    return mSegmentDirectoryVec[partitionId];
}

file_system::DirectoryPtr MultiPartSegmentDirectory::GetSegmentFsDirectory(
        segmentid_t segId, SegmentFileMetaPtr& segmentFileMeta) const
{
    size_t partitionId = 0;
    segmentid_t physicalSegId = INVALID_SEGMENTID;
    if (!DecodeVirtualSegmentId(segId, physicalSegId, partitionId))
    {
        return DirectoryPtr();
    }

    file_system::DirectoryPtr segmentParentDirectory = 
        GetSegmentParentDirectory(segId);
    if (!segmentParentDirectory)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "Failed to get segment parent dir, segId [%d]", 
                             segId);
    }
    SegmentDirectoryPtr segmentDirectory = GetSegmentDirectory(segId);
    assert(segmentDirectory);
    string segDirName = segmentDirectory->GetVersion().GetSegmentDirName(physicalSegId);
    file_system::DirectoryPtr segDirectory = 
        segmentParentDirectory->GetDirectory(segDirName, false);
    if (!segmentFileMeta->Load(segDirectory, mIsSubSegDir))
    {
        // reset for no meta optimize
        segmentFileMeta.reset();
    }

    if (mIsSubSegDir)
    {
        if (!segDirectory)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                    "Failed to get sub segment, dir [%s] in root [%s]",
                    segDirName.c_str(),
                    segmentParentDirectory->GetPath().c_str());
        }
        segDirectory = segDirectory->GetDirectory(SUB_SEGMENT_DIR_NAME, false);
    }

    bool needMountPackageFile = !segmentFileMeta || segmentFileMeta->HasPackageFile();
    if (needMountPackageFile && segDirectory)
    {
        segDirectory->MountPackageFile(PACKAGE_FILE_PREFIX);
    }
    return segDirectory;
}

void MultiPartSegmentDirectory::SetSubSegmentDir()
{
    SegmentDirectory::SetSubSegmentDir();
    for (size_t i = 0; i < mSegmentDirectoryVec.size(); ++i)
    {
        mSegmentDirectoryVec[i]->SetSubSegmentDir();
    }
}

const IndexFormatVersion& MultiPartSegmentDirectory::GetIndexFormatVersion() const
{
    if (mSegmentDirectoryVec.empty())
    {
        static IndexFormatVersion formatVersion;
        return formatVersion;
    }
    return mSegmentDirectoryVec[0]->GetIndexFormatVersion();
}

IE_NAMESPACE_END(index_base);


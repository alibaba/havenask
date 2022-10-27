#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/segment/online_segment_directory.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"
#include "indexlib/config/build_config.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SegmentDirectory);

bool SegmentDirectory::SegmentDataFinder::Find(segmentid_t segId, SegmentData& segData)
{
    if (mSegDataVec.empty() || segId > mSegDataVec.rbegin()->GetSegmentId())
    {
        return false;
    }
    if (mCursor >= mSegDataVec.size() || mSegDataVec[mCursor].GetSegmentId() > segId)
    {
        mCursor = 0;
    }
    for ( ; mCursor < mSegDataVec.size(); ++mCursor)
    {
        segmentid_t curSegId = mSegDataVec[mCursor].GetSegmentId();
        if (curSegId < segId)
        {
            continue;
        }
        if (curSegId > segId)
        {
            return false;
        }
        if (curSegId == segId)
        {
            segData = mSegDataVec[mCursor++];
            return true;
        }
    }
    return false;
}

SegmentDirectory::SegmentDirectory() 
    : mIndexFormatVersion(INDEX_FORMAT_VERSION)
    , mIsSubSegDir(false)
{
}

SegmentDirectory::SegmentDirectory(const SegmentDirectory& other)
    : mVersion(other.mVersion)
    , mOnDiskVersion(other.mOnDiskVersion)
    , mRootDir(other.mRootDir)
    , mRootDirectory(other.mRootDirectory)
    , mSegmentDatas(other.mSegmentDatas)
    , mIndexFormatVersion(other.mIndexFormatVersion)
    , mIsSubSegDir(other.mIsSubSegDir)
    , mPatchAccessor(other.mPatchAccessor)
{
    if (other.mSubSegmentDirectory)
    {
        mSubSegmentDirectory.reset(other.mSubSegmentDirectory->Clone());
    }
}

SegmentDirectory::~SegmentDirectory() 
{
}


SegmentDirectory* SegmentDirectory::Clone()
{
    autil::ScopedLock scopedLock(mLock);
    return new SegmentDirectory(*this);
}

void SegmentDirectory::Init(const file_system::DirectoryPtr& directory,
                            index_base::Version version,
                            bool hasSub)
{
    mRootDir = directory->GetPath();
    mRootDirectory = directory;

    mVersion = version;
    if (mVersion.GetVersionId() == INVALID_VERSION)
    {
        mVersion = GetLatestVersion(directory, mVersion);
    }

    mOnDiskVersion = mVersion;
    InitIndexFormatVersion(mRootDirectory);

    DoInit(directory);
    InitSegmentDatas(mVersion);

    if (hasSub)
    {
        mSubSegmentDirectory.reset(Clone());
        mSubSegmentDirectory->SetSubSegmentDir();
    }
}

void SegmentDirectory::IncLastSegmentId()
{
    autil::ScopedLock scopedLock(mLock);
    segmentid_t segId = CreateNewSegmentId();
    mVersion.SetLastSegmentId(segId);
}

void SegmentDirectory::AddSegment(segmentid_t segId, timestamp_t ts)
{
    autil::ScopedLock scopedLock(mLock);
    mVersion.AddSegment(segId);
    if (ts > mVersion.GetTimestamp())
    {
        mVersion.SetTimestamp(ts);
    }
    InitSegmentDatas(mVersion);
}

void SegmentDirectory::UpdateSchemaVersionId(schemavid_t id)
{
    autil::ScopedLock scopedLock(mLock);
    mVersion.SetSchemaVersionId(id);
}

void SegmentDirectory::SetOngoingModifyOperations(const vector<schema_opid_t>& opIds)
{
    autil::ScopedLock scopedLock(mLock);
    mVersion.SetOngoingModifyOperations(opIds);
}

void SegmentDirectory::RemoveSegments(const vector<segmentid_t>& segIds)
{
    autil::ScopedLock scopedLock(mLock);
    for (size_t i = 0; i < segIds.size(); i++)
    {
        mVersion.RemoveSegment(segIds[i]);
    }
    InitSegmentDatas(mVersion);
}

segmentid_t SegmentDirectory::ExtractSegmentId(const string& path)
{
    if (path.length() < mRootDir.length() 
        || path.compare(0, mRootDir.length(), mRootDir) != 0)
    {
        return INVALID_SEGMENTID;
    }
    
    string subPath = path.substr(mRootDir.length());
    StringTokenizer tokenizer(subPath, "/", StringTokenizer::TOKEN_IGNORE_EMPTY);
    if (tokenizer.getNumTokens() == 0)
    {
        return INVALID_SEGMENTID;
    }

    return Version::GetSegmentIdByDirName(tokenizer[0]);
}

segmentid_t SegmentDirectory::CreateNewSegmentId()
{
    autil::ScopedLock scopedLock(mLock);
    segmentid_t newSegmentId = FormatSegmentId(0);
    if (mVersion.GetLastSegment() != INVALID_SEGMENTID)
    {
        newSegmentId = mVersion.GetLastSegment() + 1;
    }
    return newSegmentId;
}

std::string SegmentDirectory::GetSegmentDirName(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    return mVersion.GetSegmentDirName(segId);
}

bool SegmentDirectory::IsVersionChanged() const
{
    autil::ScopedLock scopedLock(mLock);
    return mVersion != mOnDiskVersion;
}

void SegmentDirectory::RollbackToCurrentVersion(bool needRemoveSegment)
{
    autil::ScopedLock scopedLock(mLock);
    fslib::FileList versionList;
    VersionLoader::ListVersion(mRootDirectory, versionList);
    for (size_t i = 0; i < versionList.size(); ++i)
    {
        Version version;
        version.Load(mRootDirectory, versionList[i]);
        if (mVersion.GetVersionId() < version.GetVersionId())
        {
            mRootDirectory->RemoveFile(versionList[i]);
        }
    }

    if (!needRemoveSegment)
    {
        return;
    }
    segmentid_t lastSegment = mVersion.GetLastSegment();
    fslib::FileList segments;
    VersionLoader::ListSegments(mRootDirectory, segments);
    for (size_t i = 0; i < segments.size(); ++i)
    {
        segmentid_t currentSegment = Version::GetSegmentIdByDirName(segments[i]);
        if (lastSegment < currentSegment)
        {
            mRootDirectory->RemoveDirectory(segments[i]);
        }
    }
}

void SegmentDirectory::CommitVersion()
{
    autil::ScopedLock scopedLock(mLock);
    return DoCommitVersion(true);
}

void SegmentDirectory::DoCommitVersion(bool dumpDeployIndexMeta)
{
    if (!IsVersionChanged())
    {
        return;
    }
    SegmentDataVector::reverse_iterator it = mSegmentDatas.rbegin();
    if (it != mSegmentDatas.rend())
    {
        mVersion.SetTimestamp(it->GetSegmentInfo().timestamp);
        mVersion.SetLocator(it->GetSegmentInfo().locator);
    }

    versionid_t preVersion = mVersion.GetVersionId();
    mVersion.IncVersionId();
    if (dumpDeployIndexMeta)
    {
        VersionCommitter::DumpPartitionPatchMeta(mRootDirectory->GetPath(), mVersion);
        DeployIndexWrapper::DumpDeployMeta(mRootDirectory, mVersion);
    }
    mVersion.Store(mRootDirectory, false);
    mOnDiskVersion = mVersion;
    if (dumpDeployIndexMeta)
    {
        IndexSummary ret = IndexSummary::Load(mRootDirectory,
                mVersion.GetVersionId(), preVersion);
        ret.Commit(mRootDirectory);
    }
}

file_system::DirectoryPtr SegmentDirectory::GetSegmentFsDirectory(
        segmentid_t segId, SegmentFileMetaPtr& segmentFileMeta) const
{
    autil::ScopedLock scopedLock(mLock);
    file_system::DirectoryPtr segmentParentDirectory = 
        GetSegmentParentDirectory(segId);
    if (!segmentParentDirectory)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                             "Failed to get segment parent dir, segId [%d]", 
                             segId);
    }

    string segDirName = GetSegmentDirName(segId);
    file_system::DirectoryPtr segDirectory = 
        segmentParentDirectory->GetDirectory(segDirName, false);
    if (!segDirectory || !segmentFileMeta->Load(segDirectory, mIsSubSegDir))
    {
        // may be a rt segment, reset for no meta optimize
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

SegmentDirectoryPtr SegmentDirectory::GetSubSegmentDirectory()
{
    autil::ScopedLock scopedLock(mLock);
    return mSubSegmentDirectory; 
}

file_system::DirectoryPtr SegmentDirectory::GetSegmentParentDirectory(
        segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    return mRootDirectory;
}

void SegmentDirectory::InitSegmentDatas(index_base::Version& version)
{
    UpdatePatchAccessor(version);

    SegmentDataVector newSegmentDatas;
    exdocid_t baseDocId = 0;

    SegmentDataFinder segDataFinder(mSegmentDatas);
    for (size_t i = 0; i < version.GetSegmentCount(); ++i)
    {
        segmentid_t segId = version[i];
        SegmentData segmentData;
        if (segDataFinder.Find(segId, segmentData))
        {
            IE_LOG(DEBUG, "reuse segmentData for segment [%d]", segId);
            segmentData.SetBaseDocId(baseDocId);
            baseDocId += segmentData.GetSegmentInfo().docCount;
            segmentData.SetPatchIndexAccessor(mPatchAccessor);
        }
        else
        {
            IE_LOG(DEBUG, "load new segmentData for segment [%d]", segId);
            SegmentFileMetaPtr segmentFileMeta(new SegmentFileMeta);
            DirectoryPtr segDirectory = GetSegmentFsDirectory(segId, segmentFileMeta);
            if (!segDirectory)
            {
                INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                        "Failed to get segment directory [%d]", segId);            
            }

            SegmentInfo segmentInfo;
            if (!segmentInfo.Load(segDirectory))
            {
                INDEXLIB_FATAL_ERROR(IndexCollapsed, 
                        "Failed to load segment info [%s]",
                        segDirectory->GetPath().c_str());            
            }

            SegmentMetrics segmentMetrics;
            if (segDirectory->IsExist(SEGMETN_METRICS_FILE_NAME))
            {
                segmentMetrics.Load(segDirectory);
            }

            segmentData.SetBaseDocId(baseDocId);
            baseDocId += segmentInfo.docCount;
            segmentData.SetSegmentInfo(segmentInfo);
            segmentData.SetSegmentMetrics(segmentMetrics);
            segmentData.SetSegmentId(segId);
            segmentData.SetDirectory(segDirectory);
            segmentData.SetPatchIndexAccessor(mPatchAccessor);
            segmentData.SetSegmentFileMeta(segmentFileMeta);
        }
        newSegmentDatas.push_back(segmentData);
    }
    
    mSegmentDatas.swap(newSegmentDatas);
    SegmentDataVector::reverse_iterator it = mSegmentDatas.rbegin();
    if (it != mSegmentDatas.rend())
    {
        version.SetTimestamp(it->GetSegmentInfo().timestamp);
        version.SetLocator(it->GetSegmentInfo().locator);
    }
}

SegmentData SegmentDirectory::GetSegmentData(segmentid_t segId) const
{
    autil::ScopedLock scopedLock(mLock);
    for (size_t i = 0; i < mSegmentDatas.size(); ++i)
    {
        if (mSegmentDatas[i].GetSegmentId() == segId)
        {
            return mSegmentDatas[i];
        }
    }

    INDEXLIB_FATAL_ERROR(NonExist, "segment data [%d] not exist", segId);
    return SegmentData();
}

void SegmentDirectory::SetSubSegmentDir() 
{
    autil::ScopedLock scopedLock(mLock);
    mIsSubSegDir = true;
    mSegmentDatas.clear();
    InitSegmentDatas(mVersion);
}

BuildingSegmentData SegmentDirectory::CreateNewSegmentData(
        const BuildConfig& buildConfig)
{
    autil::ScopedLock scopedLock(mLock);
    BuildingSegmentData newSegmentData(buildConfig);
    segmentid_t segId = CreateNewSegmentId();
    newSegmentData.SetSegmentId(segId);
    newSegmentData.SetBaseDocId(0);
    newSegmentData.SetSegmentDirName(mVersion.GetNewSegmentDirName(segId));

    const SegmentDataVector& segmentDatas = GetSegmentDatas();
    SegmentDataVector::const_reverse_iterator it = segmentDatas.rbegin();
    if (it != segmentDatas.rend())
    {
        SegmentInfo lastSegmentInfo = it->GetSegmentInfo();
        SegmentInfo newSegmentInfo;
        newSegmentInfo.locator = lastSegmentInfo.locator;
        newSegmentInfo.timestamp = lastSegmentInfo.timestamp;
        newSegmentData.SetSegmentInfo(newSegmentInfo);
        newSegmentData.SetBaseDocId(it->GetBaseDocId() + lastSegmentInfo.docCount);
    }
    else
    {
        SegmentInfo newSegmentInfo;
        newSegmentInfo.locator = mVersion.GetLocator();
        newSegmentInfo.timestamp = mVersion.GetTimestamp();
        newSegmentData.SetSegmentInfo(newSegmentInfo);
    }
    return newSegmentData;
}

void SegmentDirectory::InitIndexFormatVersion(
        const DirectoryPtr& directory)
{
    assert(directory);
    try {
        mIndexFormatVersion.Load(directory);
    } catch (NonExistException& e) {
        IE_LOG(INFO, "%s not exist in rootDir [%s]!", 
               INDEX_FORMAT_VERSION_FILE_NAME, directory->GetPath().c_str());
    } catch (...) {
        throw;
    }
}

void SegmentDirectory::UpdatePatchAccessor(const Version& version)
{
    autil::ScopedLock scopedLock(mLock);
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID)
    {
        return;
    }
    
    if (mPatchAccessor && mPatchAccessor->GetVersionId() == version.GetVersionId())
    {
        return;
    }
    mPatchAccessor.reset(new PartitionPatchIndexAccessor);
    mPatchAccessor->Init(mRootDirectory, version);
    if (mIsSubSegDir)
    {
        mPatchAccessor->SetSubSegmentDir();
    }
}

void SegmentDirectory::UpdateCounterMap(const CounterMapPtr& counterMap) const
{
    autil::ScopedLock scopedLock(mLock);
    const SegmentDataVector& segDataVec = GetSegmentDatas();
    for (auto it = segDataVec.rbegin(); it != segDataVec.rend(); ++it)
    {
        if (OnlineSegmentDirectory::IsIncSegmentId(it->GetSegmentId()))
        {
            const DirectoryPtr& lastIncSegDir = it->GetDirectory();
            assert(lastIncSegDir);
            
            string counterString;
            try
            {
                lastIncSegDir->Load(COUNTER_FILE_NAME, counterString, true);
            }
            catch (const misc::NonExistException& e)
            {
                IE_LOG(WARN, "counter file not exists in directory[%s]",
                       lastIncSegDir->GetPath().c_str());
                return;
            }
            catch (...)
            {
                throw;
            }
            
            counterMap->Merge(counterString, util::CounterBase::FJT_OVERWRITE);
            return;
        }
    }
}

Version SegmentDirectory::GetLatestVersion(const DirectoryPtr& directory,
        const Version& emptyVersion) const
{
    Version version;
    VersionLoader::GetVersion(directory, version, INVALID_VERSION);
    return version.GetVersionId() == INVALID_VERSION ? emptyVersion : version;
}

IE_NAMESPACE_END(index_base);


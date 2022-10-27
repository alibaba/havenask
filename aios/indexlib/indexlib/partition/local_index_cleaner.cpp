#include <autil/StringUtil.h>
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/index_path_util.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, LocalIndexCleaner);

LocalIndexCleaner::LocalIndexCleaner(const DirectoryPtr& rootDir,
                                     uint32_t keepVersionCount,
                                     const ReaderContainerPtr& readerContainer)
    : mRootDir(rootDir)
    , mKeepVersionCount(keepVersionCount)
    , mReaderContainer(readerContainer)
{
    assert(mRootDir);
}

bool LocalIndexCleaner::Clean(const vector<versionid_t>& keepVersionIds)
{
    try
    {
        DoClean(keepVersionIds, true);
    }
    catch (...)
    {
        return false;
    }
    return true;
}

// void LocalIndexCleaner::Execute()
// {
//     // indexlib should not clean files manually for rollback versions
//     DoClean({}, false);
// }

void LocalIndexCleaner::DoClean(const vector<versionid_t>& keepVersionIds,
                                bool cleanAfterMaxKeepVersionFiles)
{
    IE_LOG(INFO, "start clean local index. keep versions [%s], cleanAfterMaxKeepVersionFiles [%d]", StringUtil::toString(keepVersionIds, ",").c_str(), cleanAfterMaxKeepVersionFiles);
    ScopedLock lock(mLock);
    set<Version> keepVersions;
    if (mReaderContainer)
    {
        vector<Version> usingVersions;
        mReaderContainer->GetIncVersions(usingVersions);
        keepVersions.insert(usingVersions.begin(), usingVersions.end());
        mVersionCache.insert(usingVersions.begin(), usingVersions.end());
    }
    for (versionid_t versionId : keepVersionIds)
    {
        keepVersions.insert(GetVersion(versionId));
    }

    FileList fileList;
    mRootDir->ListFile("", fileList, false, false, true);
    if (mKeepVersionCount != INVALID_KEEP_VERSION_COUNT &&
        keepVersions.size() < mKeepVersionCount)
    {
        versionid_t tmpVersionId;
        versionid_t currentVersionId = mReaderContainer ?
            mReaderContainer->GetLatestReaderVersion() :
            (keepVersions.empty() ? INVALID_VERSION : keepVersions.begin()->GetVersionId());
        set<versionid_t> localVersionIds;
        for (const string& name : fileList)
        {
            if (IndexPathUtil::GetVersionId(name, tmpVersionId) &&
                (currentVersionId == INVALID_VERSION || tmpVersionId < currentVersionId))
            {
                localVersionIds.insert(tmpVersionId);
            }
        }
        for (auto it = localVersionIds.rbegin();
             it != localVersionIds.rend() && keepVersions.size() < mKeepVersionCount; ++it)
        {
            keepVersions.insert(GetVersion(*it));
        }
    }
    CleanFiles(mRootDir, fileList, keepVersions, cleanAfterMaxKeepVersionFiles);
    mVersionCache.swap(keepVersions);
    IE_LOG(INFO, "finish clean local index");
}

const Version& LocalIndexCleaner::GetVersion(versionid_t versionId)
{
    auto it = mVersionCache.find(Version(versionId));
    if (it != mVersionCache.end())
    {
        return *it;
    }
    Version version;
    // final version files may be deploying
    // try
    // {
        VersionLoader::GetVersion(mRootDir, version, versionId);
    // }
    // catch (...)
    // {
    //     if (mayNotInLocal)
    //     {
    //         string secondaryRootPath = mRootDir->GetFileSystem()->GetSecondaryRootPath();
    //         IE_LOG(INFO, "version [%d] may not in local, try read from remote [%s]",
    //                versionId, path.c_str());
    //         VersionLoader::GetVersion(secondaryRootPath, version, versionId);
    //     }
    //     else
    //     {
    //         throw;
    //     }
    // }
    return *(mVersionCache.insert(version).first);
}

void LocalIndexCleaner::CleanFiles(const DirectoryPtr& rootDir, const FileList& fileList,
                                   const set<Version>& keepVersions,
                                   bool cleanAfterMaxKeepVersionFiles)
{
    set<segmentid_t> keepSegmentIds;
    set<schemavid_t> keepSchemaIds;
    for (const Version& version : keepVersions)
    {
        keepSegmentIds.insert(version.GetSegmentVector().begin(),
                              version.GetSegmentVector().end());
        keepSchemaIds.insert(version.GetSchemaVersionId());
    }
    versionid_t maxCanCleanVersionId =
        (cleanAfterMaxKeepVersionFiles || keepVersions.empty()) ?
        std::numeric_limits<versionid_t>::max() : keepVersions.rbegin()->GetVersionId();
    segmentid_t maxCanCleanSegmentId =
        (cleanAfterMaxKeepVersionFiles || keepSegmentIds.empty()) ?
        std::numeric_limits<segmentid_t>::max() : *keepSegmentIds.rbegin();
    schemavid_t maxCanCleanSchemaid =
        (cleanAfterMaxKeepVersionFiles || keepSchemaIds.empty()) ?
        std::numeric_limits<schemavid_t>::max() : *keepSchemaIds.rbegin();
    
    segmentid_t segmentId;
    versionid_t versionId;
    schemavid_t schemaId;
    for (const string& name : fileList)
    {
        if (IndexPathUtil::GetSegmentId(name, segmentId))
        {
            if (segmentId <= maxCanCleanSegmentId && keepSegmentIds.count(segmentId) == 0)
            {
                rootDir->RemoveDirectory(name);
                IE_LOG(INFO, "[%s/%s] removed", rootDir->GetPath().c_str(), name.c_str());
            }
        }
        else if (IndexPathUtil::GetVersionId(name, versionId) ||
                 IndexPathUtil::GetDeployMetaId(name, versionId) ||
                 IndexPathUtil::GetPatchMetaId(name, versionId))
        {
            if (versionId <= maxCanCleanVersionId && keepVersions.count(Version(versionId)) == 0)
            {
                rootDir->RemoveFile(name);
                IE_LOG(INFO, "[%s/%s] removed", rootDir->GetPath().c_str(), name.c_str());
            }
        }
        else if (IndexPathUtil::GetSchemaId(name, schemaId))
        {
            if (schemaId <= maxCanCleanSchemaid && keepSchemaIds.count(schemaId) == 0)
            {
                rootDir->RemoveFile(name);
                IE_LOG(INFO, "[%s/%s] removed", rootDir->GetPath().c_str(), name.c_str());
            }
        }
        else if (IndexPathUtil::GetPatchIndexId(name, schemaId))
        {
            CleanPatchIndexSegmentFiles(
                rootDir, name, keepSegmentIds, maxCanCleanSegmentId, schemaId);
        }
    }
}

void LocalIndexCleaner::CleanPatchIndexSegmentFiles(
    const DirectoryPtr& rootDir, const string& patchDirName,
    const set<segmentid_t>& keepSegmentIds, segmentid_t maxCanCleanSegmentId,
    schemavid_t patchSchemaId)
{
    if (unlikely(patchSchemaId == DEFAULT_SCHEMAID)) // Why? for legency code, may useless
    {
        assert(false);
        return;
    }
    FileList segmentList;
    rootDir->ListFile(patchDirName, segmentList, false, false, true);
    
    DirectoryPtr patchDir = rootDir->GetDirectory(patchDirName, true);
    assert(patchDir);
    bool canRemovePatchDir = true;
    for (const string& segmentName : segmentList)
    {
        segmentid_t segmentId;
        bool tmpIsSuccess = IndexPathUtil::GetSegmentId(segmentName, segmentId);
        assert(tmpIsSuccess);(void)tmpIsSuccess;
        if (segmentId <= maxCanCleanSegmentId && keepSegmentIds.count(segmentId) == 0)
        {
            patchDir->RemoveDirectory(segmentName);
            IE_LOG(INFO, "Patch index segment [%s/%s/%s] removed",
                   rootDir->GetPath().c_str(), patchDirName.c_str(), segmentName.c_str());
        }
        else
        {
            canRemovePatchDir = false;
        }
    }
    if (canRemovePatchDir)
    {
        rootDir->RemoveDirectory(patchDirName);
        IE_LOG(INFO, "Empty patch index path [%s] removed", patchDirName.c_str());
    }
}

IE_NAMESPACE_END(partition);


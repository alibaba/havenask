#include "indexlib/partition/on_disk_index_cleaner.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"

using namespace std;
using namespace fslib;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnDiskIndexCleaner);

OnDiskIndexCleaner::OnDiskIndexCleaner(
        uint32_t keepVersionCount,
        const ReaderContainerPtr& readerContainer,
        const DirectoryPtr& directory)
    : mKeepVersionCount(keepVersionCount)
    , mReaderContainer(readerContainer)
    , mDirectory(directory)
    , mLocalOnly(true)
{
}

OnDiskIndexCleaner::~OnDiskIndexCleaner()
{
}

void OnDiskIndexCleaner::Execute()
{
    IE_LOG(DEBUG, "rootDir:%s, keepVersionCount:%u",
           mDirectory->GetPath().c_str(), mKeepVersionCount);

    if (mKeepVersionCount == INVALID_KEEP_VERSION_COUNT)
    {
        return;
    }
    if (mKeepVersionCount == 0)
    {
        IE_LOG(WARN, "Keep version count should not be less than 0. "
               "use 1 by default.");
        mKeepVersionCount = 1;
    }

    fslib::FileList fileList;
    VersionLoader::ListVersion(mDirectory, fileList, true);
    if (fileList.size() <= mKeepVersionCount)
    {
        return;
    }

    set<segmentid_t> needKeepSegments;
    set<schemavid_t> needKeepSchemaId;
    size_t firstKeepVersionIdx = 0;
    ConstructNeedKeepVersionIdxAndSegments(fileList, firstKeepVersionIdx,
            needKeepSegments, needKeepSchemaId);

    segmentid_t maxSegId = CalculateMaxSegmentIdInAllVersions(fileList);

    CleanVersionFiles(firstKeepVersionIdx, fileList);
    CleanSegmentFiles(mDirectory, maxSegId, needKeepSegments);
    CleanPatchIndexSegmentFiles(maxSegId, needKeepSegments);
    CleanUselessSchemaFiles(needKeepSchemaId);
}

void OnDiskIndexCleaner::ConstructNeedKeepVersionIdxAndSegments(
        const fslib::FileList& fileList, 
        size_t& firstKeepVersionIdx,
        set<segmentid_t>& needKeepSegments,
        set<schemavid_t>& needKeepSchemaId)
{
    firstKeepVersionIdx = 0;
    while (firstKeepVersionIdx < fileList.size() - mKeepVersionCount)
    {
        Version version;
        version.Load(mDirectory, fileList[firstKeepVersionIdx]);
        if (mReaderContainer
            && mReaderContainer->HasReader(version.GetVersionId()))
        {
            break;
        }
        firstKeepVersionIdx++;
    }
    for (size_t i = firstKeepVersionIdx; i < fileList.size(); ++i)
    {
        Version version;
        version.Load(mDirectory, fileList[i]);
        for (size_t j = 0; j < version.GetSegmentCount(); ++j)
        {
            needKeepSegments.insert(version[j]);
        }
        needKeepSchemaId.insert(version.GetSchemaVersionId());
    }
}

segmentid_t OnDiskIndexCleaner::CalculateMaxSegmentIdInAllVersions(
        const fslib::FileList& fileList)
{
    segmentid_t maxSegIdInAllVersions = INVALID_SEGMENTID;
    // when version rollbacks(e.g. v0:[0,1,2], v1:[0,1], v1 is a roll 
    // backed version), we must scan all version
    for (size_t i = 0; i < fileList.size(); ++i)
    {
        Version version;
        version.Load(mDirectory, fileList[i]);
        segmentid_t lastSegId = version.GetLastSegment();
        if (lastSegId != INVALID_SEGMENTID
            && lastSegId > maxSegIdInAllVersions)
        {
            maxSegIdInAllVersions = lastSegId;
        }
    }
    return maxSegIdInAllVersions;
}

void OnDiskIndexCleaner::CleanVersionFiles(size_t firstKeepVersionIdx,
        const fslib::FileList& fileList)
{
    for (size_t i = 0; i < firstKeepVersionIdx; ++i)
    {
        versionid_t versionId = VersionLoader::GetVersionId(fileList[i]);
        string deployMetaFile = DeployIndexWrapper::GetDeployMetaFileName(versionId);
        mDirectory->RemoveFile(deployMetaFile, true);

        string patchMetaFile = PartitionPatchMeta::GetPatchMetaFileName(versionId);
        mDirectory->RemoveFile(patchMetaFile, true);
        mDirectory->RemoveFile(fileList[i]);
        IE_LOG(INFO, "Version: [%s/%s] removed",
               mDirectory->GetPath().c_str(), fileList[i].c_str());
    }
}

void OnDiskIndexCleaner::CleanSegmentFiles(const file_system::DirectoryPtr& dir,
        segmentid_t maxSegIdInAllVersions, const set<segmentid_t>& needKeepSegments)
{
    fslib::FileList segList;
    VersionLoader::ListSegments(dir, segList, mLocalOnly);
    for (fslib::FileList::const_iterator it = segList.begin(); 
         it != segList.end(); ++it)
    {
        string segmentDirName = *it;
        segmentid_t segId = Version::GetSegmentIdByDirName(segmentDirName);
        if (segId <= maxSegIdInAllVersions
            && needKeepSegments.find(segId) == needKeepSegments.end())
        {
            dir->RemoveDirectory(segmentDirName);
            IE_LOG(INFO, "Segment: [%s/%s] removed",
                   dir->GetPath().c_str(), segmentDirName.c_str());
        }
    }
}

void OnDiskIndexCleaner::CleanPatchIndexSegmentFiles(segmentid_t maxSegInVersion,
        const set<segmentid_t>& needKeepSegment)
{
    FileList patchIndexDirList;
    PartitionPatchIndexAccessor::ListPatchRootDirs(mDirectory, patchIndexDirList, mLocalOnly);
    for (size_t i = 0; i < patchIndexDirList.size(); i++)
    {
        DirectoryPtr patchDir = mDirectory->GetDirectory(patchIndexDirList[i], false);
        if (!patchDir)
        {
            continue;
        }

        CleanSegmentFiles(patchDir, maxSegInVersion, needKeepSegment);
        FileList tmpList;
        mDirectory->ListFile(patchIndexDirList[i], tmpList, false, false, mLocalOnly);
        if (tmpList.empty())
        {
            IE_LOG(INFO, "Removing empty patch index path [%s]", patchIndexDirList[i].c_str());
            mDirectory->RemoveDirectory(patchIndexDirList[i]);
        }
    }
}

void OnDiskIndexCleaner::CleanUselessSchemaFiles(const set<schemavid_t>& needKeepSchemaId)
{
    if (needKeepSchemaId.empty())
    {
        IE_LOG(WARN, "no valid schema id to keep!");
        return;
    }
    
    schemavid_t maxSchemaId = *(needKeepSchemaId.rbegin());
    FileList tmpList;
    mDirectory->ListFile("", tmpList, false, false, mLocalOnly);
    for (const auto& file : tmpList)
    {
        schemavid_t schemaId = DEFAULT_SCHEMAID;
        if (!Version::ExtractSchemaIdBySchemaFile(file, schemaId))
        {
            continue;
        }
        if (schemaId < maxSchemaId &&
            needKeepSchemaId.find(schemaId) == needKeepSchemaId.end())
        {
            IE_LOG(INFO, "Removing schema file: [%s]", file.c_str());
            mDirectory->RemoveFile(file, true);
        }
    }
}

IE_NAMESPACE_END(partition);


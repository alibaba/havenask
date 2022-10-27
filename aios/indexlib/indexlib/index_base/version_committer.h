#ifndef __INDEXLIB_VERSION_COMMITTER_H
#define __INDEXLIB_VERSION_COMMITTER_H

#include <tr1/memory>
#include <set>
#include <fslib/common/common_type.h>
#include <beeper/beeper.h>
#include "indexlib/indexlib.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/partition_patch_meta_creator.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/index_summary.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/file_system/file_system_adapter.h"
#include "indexlib/util/path_util.h"

IE_NAMESPACE_BEGIN(index_base);

class VersionCommitter
{
public:
    VersionCommitter(const std::string& rootDir, 
                     const Version& version,
                     uint32_t keepVersionCount,
                     const std::set<versionid_t>& reservedVersionSet = {})
        : mVersion(version)
        , mKeepVersionCount(keepVersionCount)
        , mReservedVersionSet(reservedVersionSet)
    {
        mRootDir = storage::FileSystemWrapper::NormalizeDir(rootDir);
        mFsRootDir = file_system::DirectoryPtr();
    }        
        
    VersionCommitter(const file_system::DirectoryPtr& rootDir, 
                     const Version& version,
                     uint32_t keepVersionCount,
                     const std::set<versionid_t>& reservedVersionSet = {})
        : mFsRootDir(rootDir)
        , mVersion(version)
        , mKeepVersionCount(keepVersionCount)
        , mReservedVersionSet(reservedVersionSet)
    {
        mRootDir = storage::FileSystemWrapper::NormalizeDir(mFsRootDir->GetPath());
    }
    
    ~VersionCommitter() {}
public:
    void Commit()
    {
        if (mFsRootDir)
        {            
            DumpPartitionPatchMeta(mFsRootDir, mVersion);
            DeployIndexWrapper::DumpDeployMeta(mFsRootDir, mVersion);
            mVersion.Store(mFsRootDir, true); 
            CleanVersions(mFsRootDir, mKeepVersionCount, mReservedVersionSet);
            return;
        }
        DumpPartitionPatchMeta(mRootDir, mVersion);
        DeployIndexWrapper::DumpDeployMeta(mRootDir, mVersion);
        mVersion.Store(mRootDir, true);
        CleanVersions(mRootDir, mKeepVersionCount, mReservedVersionSet);
    }        

    
public:
    template <typename PathType>
    static void CleanVersions(const PathType& rootDir,
                              uint32_t keepVersionCount,
                              const std::set<versionid_t>& reservedVersionSet = {});

    // will clean version and versions small than version, but can not clean all version.
    template <typename PathType>
    static void DumpPartitionPatchMeta(const PathType& rootDir, const Version& version);

    template <typename PathType>
    static bool CleanVersionAndBefore(const PathType& rootDir,
                                      versionid_t versionId,
                                      const std::set<versionid_t>& reservedVersionSet= {});

private:
    template <typename PathType> 
    static void CleanVersions(IndexSummary& summary, const PathType& rootDir,
                              const fslib::FileList& fileList,
                              uint32_t keepVersionCount,
                              const std::set<versionid_t>& reservedVersionSet);

    template <typename PathType>     
    static segmentid_t ConstructNeedKeepSegment(
            const PathType& rootDir, const fslib::FileList& fileList,
            const std::set<versionid_t>& reservedVersionSet, uint32_t keepVersionCount, 
            std::set<segmentid_t>& needKeepSegment, std::set<schemavid_t>& needKeepSchemaId);
    
    template <typename PathType>         
    static void CleanVersionFiles(
            const PathType& rootDir, const fslib::FileList& fileList,
            const std::set<versionid_t>& reservedVersionSet, uint32_t keepVersionCount);
    
    template <typename PathType> 
    static std::vector<segmentid_t> CleanSegmentFiles(
            const PathType& rootDir, segmentid_t maxSegInVersion,
            const std::set<segmentid_t>& needKeepSegment);

    template <typename PathType> 
    static void CleanPatchIndexSegmentFiles(
            const PathType& normRootDir, segmentid_t maxSegInVersion,
            const std::set<segmentid_t>& needKeepSegment);

    template <typename PathType> 
    static void CleanUselessSchemaFiles(
            const PathType& normRootDir, const std::set<schemavid_t>& needKeepSchemaId);

private:
    std::string mRootDir;
    file_system::DirectoryPtr mFsRootDir;
    Version mVersion;
    uint32_t mKeepVersionCount;
    std::set<versionid_t> mReservedVersionSet;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VersionCommitter);

////////////////////////////////////////////////////////////////////////////////

template <typename PathType>
void VersionCommitter::CleanVersions(const PathType& rootDir,
                                     uint32_t keepVersionCount,
                                     const std::set<versionid_t>& reservedVersionSet)
{
    IE_LOG(DEBUG, "rootDir:%s, keepVersionCount:%u",
           file_system::FileSystemAdapter<PathType>::GetPathName(rootDir).c_str(), keepVersionCount);

    if (keepVersionCount == INVALID_KEEP_VERSION_COUNT)
    {
        return;
    }
    else if (keepVersionCount == 0)
    {
        IE_LOG(WARN, "Keep version count should not be less than 0. "
               "use 1 by default.");
        keepVersionCount = 1;
    }

    fslib::FileList fileList;
    VersionLoader::ListVersion(rootDir, fileList);
    IndexSummary summary = IndexSummary::Load(rootDir, fileList);
    if (fileList.size() <= keepVersionCount)
    {
        summary.Commit(rootDir);
        return;
    }
    CleanVersions(summary, rootDir, fileList, keepVersionCount, reservedVersionSet);
    summary.Commit(rootDir);
}


template <typename PathType> 
void VersionCommitter::CleanVersions(IndexSummary& indexSummary,
                                     const PathType& rootDir,
                                     const fslib::FileList& fileList,
                                     uint32_t keepVersionCount,
                                     const std::set<versionid_t>& reservedVersionSet)
{
    std::set<segmentid_t> needKeepSegment;
    std::set<schemavid_t> needKeepSchemaId;
    segmentid_t maxSegInVersion = ConstructNeedKeepSegment(rootDir,
            fileList, reservedVersionSet, keepVersionCount,
            needKeepSegment, needKeepSchemaId);

    CleanVersionFiles(rootDir, fileList, reservedVersionSet,
                      keepVersionCount);

    std::vector<segmentid_t> removeSegIds = CleanSegmentFiles(
            rootDir, maxSegInVersion, needKeepSegment);
    for (auto segId : removeSegIds)
    {
        indexSummary.RemoveSegment(segId);
    }
    CleanPatchIndexSegmentFiles(rootDir, maxSegInVersion, needKeepSegment);
    CleanUselessSchemaFiles(rootDir, needKeepSchemaId);
}

template <typename PathType>     
segmentid_t VersionCommitter::ConstructNeedKeepSegment(
    const PathType& rootDir, const fslib::FileList& fileList,
    const std::set<versionid_t>& reservedVersionSet, uint32_t keepVersionCount, 
    std::set<segmentid_t>& needKeepSegment,
    std::set<schemavid_t>& needKeepSchemaId)
{
    // Determine which segments need to keep
    for (size_t i = 0; i < keepVersionCount; ++i)
    {
        Version version;
        versionid_t versionId = VersionLoader::GetVersionId(
            fileList[fileList.size() - i - 1]);
        VersionLoader::GetVersion(rootDir, version, versionId); 
        for (size_t j = 0; j < version.GetSegmentCount(); ++j)
        {
            needKeepSegment.insert(version[j]);
        }
        needKeepSchemaId.insert(version.GetSchemaVersionId());
    }

    for (const auto& versionId : reservedVersionSet)
    {
        Version version;
        VersionLoader::GetVersion(rootDir, version, versionId);
        for (size_t j = 0; j < version.GetSegmentCount(); ++j)
        {
            needKeepSegment.insert(version[j]);
        }
        needKeepSchemaId.insert(version.GetSchemaVersionId());
    }

    if (needKeepSegment.size() == 0)
    {
        IE_LOG(WARN, "No segments in kept versions, "
               "keep count: [%u]", keepVersionCount);
    }

    // get maxSegInVersion
    segmentid_t maxSegInVersion = INVALID_SEGMENTID;
    if (!needKeepSegment.empty())
    {
        maxSegInVersion = *(needKeepSegment.rbegin());
    }
    else 
    {
        size_t versionCountToClean = fileList.size() - keepVersionCount;
        for (int32_t i = (int32_t)(versionCountToClean - 1); i >= 0; i--)
        {
            versionid_t versionId = VersionLoader::GetVersionId(fileList[i]);
            Version version;
            VersionLoader::GetVersion(rootDir, version, versionId);
            segmentid_t lastSegId = version.GetLastSegment();
            if (lastSegId != INVALID_SEGMENTID)
            {
                maxSegInVersion = lastSegId;
                break;
            }
        }
    }
    return maxSegInVersion;    
}

template <typename PathType>         
void VersionCommitter::CleanVersionFiles(
    const PathType& rootDir, const fslib::FileList& fileList,
    const std::set<versionid_t>& reservedVersionSet, uint32_t keepVersionCount)
{
    size_t versionCountToClean = fileList.size() - keepVersionCount;
    for (size_t i = 0; i < versionCountToClean; ++i)
    {
        versionid_t versionId = VersionLoader::GetVersionId(fileList[i]);
        if (reservedVersionSet.find(versionId) != reservedVersionSet.end())
        {
            continue;
        }
        IE_LOG(INFO, "Removing [%s] file.", fileList[i].c_str());
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                "Remove version [%d]", versionId);
        
        file_system::FileSystemAdapter<PathType>::DeleteFile(rootDir, fileList[i]);
        file_system::FileSystemAdapter<PathType>::DeleteFileIfExist(
                rootDir, DeployIndexWrapper::GetDeployMetaFileName(versionId));
        file_system::FileSystemAdapter<PathType>::DeleteFileIfExist(
                rootDir, index_base::PartitionPatchMeta::GetPatchMetaFileName(versionId));
        file_system::FileSystemAdapter<PathType>::DeleteFileIfExist(
                rootDir, util::PathUtil::JoinPath(INDEX_SUMMARY_INFO_DIR_NAME,
                        index_base::IndexSummary::GetFileName(versionId)));
    }
}


template <typename PathType> 
void VersionCommitter::CleanPatchIndexSegmentFiles(
        const PathType& rootDir, segmentid_t maxSegInVersion,
        const std::set<segmentid_t>& needKeepSegment)
{
    fslib::FileList patchIndexDirList;
    index_base::PartitionPatchIndexAccessor::ListPatchRootDirs(rootDir, patchIndexDirList);
    for (size_t i = 0; i < patchIndexDirList.size(); i++)
    {
        PathType patchPath = file_system::FileSystemAdapter<PathType>::GetPath(rootDir, patchIndexDirList[i]);
        CleanSegmentFiles(patchPath, maxSegInVersion, needKeepSegment);

        fslib::FileList tmpList;
        file_system::FileSystemAdapter<PathType>::ListDir(patchPath, tmpList, false);
        if (tmpList.empty())
        {
            IE_LOG(INFO, "Removing empty patch index path [%s]",
                   file_system::FileSystemAdapter<PathType>::GetPathName(patchPath).c_str());
            file_system::FileSystemAdapter<PathType>::DeleteDir(rootDir, patchIndexDirList[i]);
        }
    }
}

template <typename PathType> 
std::vector<segmentid_t> VersionCommitter::CleanSegmentFiles(
    const PathType& rootDir, segmentid_t maxSegInVersion,
    const std::set<segmentid_t>& needKeepSegment)
{
    std::vector<segmentid_t> removeSegIds;
    fslib::FileList segList;
    VersionLoader::ListSegments(rootDir, segList);

    // Clean segment files                                  
    for (fslib::FileList::const_iterator it = segList.begin(); 
         it != segList.end(); ++it)
    {
        std::string segFileName = *it;
        segmentid_t segId = Version::GetSegmentIdByDirName(segFileName);
        if (segId < maxSegInVersion 
            && needKeepSegment.find(segId) == needKeepSegment.end())
        {
            removeSegIds.push_back(segId);
            try
            {
                file_system::FileSystemAdapter<PathType>::DeleteDir(rootDir, segFileName);
            }
            catch (const misc::NonExistException& e)
            {
                IE_LOG(WARN, "Segment: [%s] already removed.", segFileName.c_str());
                continue;
            }
            IE_LOG(INFO, "Segment: [%s] removed", segFileName.c_str());
            BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                    "Segment: [%s] removed", segFileName.c_str());
        }
    }
    return removeSegIds;
}

template <typename PathType>
bool VersionCommitter::CleanVersionAndBefore(const PathType& rootDir,
                                             versionid_t versionId,
                                             const std::set<versionid_t>& reservedVersionSet)
{
    const std::string& rootDirPath = file_system::FileSystemAdapter<PathType>::GetPathName(rootDir);
    IE_LOG(INFO, "clean version[%d] and before it in partitionDir[%s]",
           versionId, rootDirPath.c_str());

    fslib::FileList fileList;
    VersionLoader::ListVersion(rootDir, fileList);
    IndexSummary summary = IndexSummary::Load(rootDir, fileList);
    uint32_t keepVersionCount = 0;
    for (fslib::FileList::const_iterator it = fileList.begin();
         it != fileList.end(); ++it)
    {
        versionid_t curVersionId = VersionLoader::GetVersionId(*it);
        if (versionId < curVersionId)
        {
            ++keepVersionCount;
        }
    }
    if (keepVersionCount == 0)
    {
        summary.Commit(rootDir);
        IE_LOG(ERROR, "can not clean all version, version[%d] in partitionDir[%s]",
               versionId, rootDirPath.c_str());
        return false;
    }
    CleanVersions(summary, rootDir, fileList, keepVersionCount, reservedVersionSet);
    summary.Commit(rootDir);
    return true;        
}

template <typename PathType>
void VersionCommitter::CleanUselessSchemaFiles(
        const PathType& rootDir, const std::set<schemavid_t>& needKeepSchemaId)
{
    if (needKeepSchemaId.empty())
    {
        IE_LOG(WARN, "no valid schema id to keep!");
        return;
    }

    schemavid_t maxSchemaId = *(needKeepSchemaId.rbegin());
    fslib::FileList schemaList;
    SchemaAdapter::ListSchemaFile(rootDir, schemaList);
    for (const auto& schemaFile : schemaList)
    {
        schemavid_t schemaId = DEFAULT_SCHEMAID;
        Version::ExtractSchemaIdBySchemaFile(schemaFile, schemaId);
        if (schemaId < maxSchemaId &&
            needKeepSchemaId.find(schemaId) == needKeepSchemaId.end())
        {
            if (!file_system::FileSystemAdapter<PathType>::IsFileExist(rootDir, schemaFile))
            {
                IE_LOG(WARN, "schema file: [%s] already removed.", schemaFile.c_str());
                continue;
            }
            file_system::FileSystemAdapter<PathType>::DeleteFile(rootDir, schemaFile);
            IE_LOG(INFO, "schema file: [%s] removed", schemaFile.c_str());
            BEEPER_FORMAT_REPORT_WITHOUT_TAGS(INDEXLIB_BUILD_INFO_COLLECTOR_NAME,
                    "schema file [%s] removed", schemaFile.c_str());
        }
    }
}

template <typename PathType>
void VersionCommitter::DumpPartitionPatchMeta(
        const PathType& rootDir, const Version& version)
{
    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID)
    {
        return;
    }
    if (!file_system::FileSystemAdapter<PathType>::IsFileExist(rootDir, version.GetSchemaFileName()))
    {
        return;
    }
    
    config::IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(
            rootDir, version.GetSchemaFileName());
    index_base::PartitionPatchMetaPtr patchMeta =
        index_base::PartitionPatchMetaCreator::Create(rootDir, schema, version);
    if (patchMeta)
    {
        patchMeta->Store(rootDir, version.GetVersionId());
    }
}

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_VERSION_COMMITTER_H

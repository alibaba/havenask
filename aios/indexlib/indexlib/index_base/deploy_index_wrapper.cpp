#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <time.h>
#include "indexlib/index_define.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/index_meta/index_file_list.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/online_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, DeployIndexWrapper);

DeployIndexWrapper::DeployIndexWrapper(
        const std::string& newPath, const std::string& lastPath,
        const OnlineConfig& onlineConfig)
    : mNewPath(newPath)
    , mLastPath(lastPath)
    , mOnlineConfig(onlineConfig)
{
    if (lastPath.empty())
    {
        mLastPath = newPath;
    }
}

DeployIndexWrapper::~DeployIndexWrapper()
{
}

bool DeployIndexWrapper::DumpSegmentDeployIndex(
    const std::string& segmentPath, const fslib::FileList& fileList, const std::string& lifecycle)
{
    time_t t_start = time(NULL);
    bool result = SegmentFileListWrapper::Dump(segmentPath, fileList, lifecycle);
    time_t t_end = time(NULL);
    IE_LOG(INFO, "Deploy segment[%s] index time [%lf]",
           segmentPath.c_str(), difftime(t_end, t_start));
    return result;    
}

bool DeployIndexWrapper::DumpSegmentDeployIndex(
    const string& segDirName, const string& lifecycle)
{
    time_t t_start = time(NULL);
    string segmentPath = FileSystemWrapper::JoinPath(
            mNewPath, segDirName);
    bool result = SegmentFileListWrapper::Dump(segmentPath, lifecycle);
    time_t t_end = time(NULL);
    IE_LOG(INFO, "Deploy segment[%s] index time [%lf]",
           segDirName.c_str(), difftime(t_end, t_start));
    return result;
}

bool DeployIndexWrapper::DumpTruncateMetaIndex()
{
    time_t t_start = time(NULL);
    string truncateMetaPath = FileSystemWrapper::JoinPath(
            mNewPath, TRUNCATE_META_DIR_NAME);
    if (FileSystemWrapper::IsExist(truncateMetaPath))
    {
        bool result = SegmentFileListWrapper::Dump(truncateMetaPath);
        time_t t_end = time(NULL);
        IE_LOG(INFO, "Deploy truncate index time [%lf]",
               difftime(t_end, t_start));
        return result;
    }
    return true;
}

bool DeployIndexWrapper::DumpAdaptiveBitmapMetaIndex()
{
    time_t t_start = time(NULL);
    string adaptiveDictPath = FileSystemWrapper::JoinPath(
            mNewPath, ADAPTIVE_DICT_DIR_NAME);
    if (FileSystemWrapper::IsExist(adaptiveDictPath))
    {
        bool result = SegmentFileListWrapper::Dump(adaptiveDictPath);
        time_t t_end = time(NULL);
        IE_LOG(INFO, "Deploy adaptive bitmap index time [%lf]",
               difftime(t_end, t_start));
        return result;
    }
    return true;
}

void DeployIndexWrapper::DumpSegmentDeployIndex(
    const DirectoryPtr& directory, const SegmentInfoPtr& segmentInfo, const std::string& lifecycle)
{
    assert(directory);
    FileList fileList;
    directory->ListFile("", fileList, true, true);
    SegmentFileListWrapper::Dump(directory, fileList, segmentInfo, lifecycle);
}

bool DeployIndexWrapper::GetDeployFiles(FileList& fileList,
        const versionid_t newVersion,
        const versionid_t lastVersion)
{
    try
    {
        int64_t totalLength = 0;
        return DoGetDeployFiles(fileList, totalLength, newVersion, lastVersion);
    }
    catch (const exception &ex)
    {
        IE_LOG(ERROR, "GetDeployFiles fail:%s", ex.what());
        return false;
    }
    catch (...)
    {
        IE_LOG(ERROR, "GetDeployFiles fail, unknown exception");
        return false;
    }
}

bool DeployIndexWrapper::GetIndexSize(int64_t &totalLength,
        const versionid_t newVersion, const versionid_t lastVersion)
{
    try
    {
        totalLength = 0;
        FileList fileList;
        return DoGetDeployFiles(fileList, totalLength, newVersion, lastVersion);
    }
    catch (const exception &ex)
    {
        IE_LOG(ERROR, "GetDeployFiles fail:%s", ex.what());
        return false;
    }
    catch (...)
    {
        IE_LOG(ERROR, "GetDeployFiles fail, unknown exception");
        return false;
    }
}

bool DeployIndexWrapper::DoGetDeployFiles(
        FileList &fileList, int64_t &totalLength,
        const versionid_t newVersionId, const versionid_t lastVersionId)
{
    if (newVersionId == INVALID_VERSION)
    {
        IE_LOG(ERROR, "invalid version id [%d]!", newVersionId);
        return false;
    }

    string newVersionPath = FileSystemWrapper::JoinPath(mNewPath,
            Version::GetVersionFileName(newVersionId));
    Version newVersion;
    try
    {
        VersionLoader::GetVersion(newVersionPath, newVersion);
    }
    catch (const exception &ex)
    {
        IE_LOG(ERROR, "get version [%s]: %s", newVersionPath.c_str(), ex.what());
        return false;
    }

    if (lastVersionId == INVALID_VERSION)
    {
        string newIndexFileListPath = FileSystemWrapper::JoinPath(
                mNewPath, DeployIndexWrapper::GetDeployMetaFileName(newVersionId));
        if (FileSystemWrapper::IsExist(newIndexFileListPath))
        {
            IndexFileList deployIndexMeta;
            if (!deployIndexMeta.Load(newIndexFileListPath)) {
                return false;
            }
            for (auto& deployFileMeta : deployIndexMeta.deployFileMetas)
            {
                if (NeedDeploy(deployFileMeta.filePath, deployIndexMeta.lifecycle))
                {
                    fileList.push_back(deployFileMeta.filePath);
                    totalLength += (deployFileMeta.fileLength != -1) ? deployFileMeta.fileLength : 0;
                }
            }
            return true;
        }

        // get diff segment files
        fileList.push_back(INDEX_FORMAT_VERSION_FILE_NAME);
        fileList.push_back(newVersion.GetSchemaFileName());
        string partitionMetaPath = FileSystemWrapper::JoinPath(
                mNewPath, INDEX_PARTITION_META_FILE_NAME);
        if (FileSystemWrapper::IsExist(partitionMetaPath))
        {
            fileList.push_back(INDEX_PARTITION_META_FILE_NAME);
        }
        if (!GetTruncateMetaDeployFiles(fileList, totalLength))
        {
            return false;
        }
        if (!GetAdaptiveBitmapMetaDeployFiles(fileList, totalLength))
        {
            return false;
        }
    }

    vector<segmentid_t> segIds;
    if (!GetDiffSegmentIds(segIds, newVersion, newVersionId, lastVersionId))
    {
        return false;
    }
    if (!GetSegmentDeployFiles(newVersion, segIds, fileList, totalLength))
    {
        return false;
    }

    if (!GetPatchSegmentDeployFiles(newVersion, lastVersionId, segIds, fileList, totalLength))
    {
        return false;
    }

    string deployMetaFileName = GetDeployMetaFileName(newVersion.GetVersionId());
    string deployMetaFilePath = FileSystemWrapper::JoinPath(mNewPath, deployMetaFileName);
    if (FileSystemWrapper::IsExist(deployMetaFilePath))
    {
        fileList.push_back(deployMetaFileName);
    }
    return true;
}

bool DeployIndexWrapper::GetSegmentSize(const SegmentData& segmentData,
                                        bool includePatch, int64_t& totalLength)
{
    if (!GetSegmentSize(segmentData.GetDirectory(), includePatch, totalLength))
    {
        return false;
    }

    const PartitionPatchIndexAccessorPtr& patchAccessor = segmentData.GetPatchIndexAccessor();
    if (!patchAccessor)
    {
        return true;
    }
    const PartitionPatchMeta& patchMeta = patchAccessor->GetPartitionPatchMeta();
    DirectoryPtr rootDir = patchAccessor->GetRootDirectory();
    const Version& version = patchAccessor->GetVersion();
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext())
    {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++)
        {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (segmentData.GetSegmentId() != segMeta.GetSegmentId())
            {
                continue;
            }
            string patchPath = FileSystemWrapper::JoinPath(rootDir->GetPath(),
                    PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()));
            string patchSegPath = FileSystemWrapper::JoinPath(patchPath,
                    version.GetSegmentDirName(segMeta.GetSegmentId()));

            IE_LOG(INFO, "target segment [%d] in [%s]", segmentData.GetSegmentId(),
                   patchSegPath.c_str());
            int64_t patchLen = 0;
            if (!GetSegmentSize(patchSegPath, includePatch, patchLen))
            {
                return false;
            }
            totalLength += patchLen;
        }
    }
    return true;
}

bool DeployIndexWrapper::DoGetSegmentSize(const IndexFileList& dpIndexMeta,
        bool includePatch, int64_t& totalLength)
{
    totalLength = 0;
    for (size_t i = 0; i < dpIndexMeta.deployFileMetas.size(); ++i)
    {
        if (!includePatch && index_base::PatchFileFinder::IsAttrPatchFile(
                util::PathUtil::GetFileName(dpIndexMeta.deployFileMetas[i].filePath)))
        {
            continue;
        }
        
        if (dpIndexMeta.deployFileMetas[i].fileLength > 0)
        {
            totalLength += dpIndexMeta.deployFileMetas[i].fileLength;
        }
    }
    return true;
}

bool DeployIndexWrapper::GetFiles(
        const string &relativePath, fslib::FileList &fileList, int64_t &totalLength)
{
    string absPath = FileSystemWrapper::JoinPath(mNewPath, relativePath);
    IndexFileList dpIndexMeta;
    if (!SegmentFileListWrapper::Load(absPath, dpIndexMeta))
    {
        return false;
    }

    for (size_t i = 0; i < dpIndexMeta.deployFileMetas.size(); ++i)
    {
        string filePath = FileSystemWrapper::JoinPath(relativePath,
                dpIndexMeta.deployFileMetas[i].filePath);
        if (NeedDeploy(filePath, dpIndexMeta.lifecycle))
        {
            fileList.push_back(filePath);
            if (dpIndexMeta.deployFileMetas[i].fileLength > 0)
            {
                totalLength += dpIndexMeta.deployFileMetas[i].fileLength;
            }
        }
    }
    string dpIndexMetaPath = FileSystemWrapper::JoinPath(relativePath, dpIndexMeta.GetFileName());
    if (NeedDeploy(dpIndexMetaPath, dpIndexMeta.lifecycle))
    {
        fileList.push_back(dpIndexMetaPath);
    }
    return true;
}

bool DeployIndexWrapper::NeedDeploy(const string& filePath, const string& lifecycle)
{
    // TODO : support package file
    assert(!filePath.empty());
    const auto& loadConfigs = mOnlineConfig.loadConfigList.GetLoadConfigs();
    for (const auto& loadConfig : loadConfigs)
    {
        if (loadConfig.Match(filePath, lifecycle))
        {
            return loadConfig.NeedDeploy();
        }
    }
    return mOnlineConfig.loadConfigList.GetDefaultLoadConfig().NeedDeploy();
}

const LoadConfig& DeployIndexWrapper::GetLoadConfig(const OnlineConfig& onlineConfig,
        const string& filePath, const string& lifecycle)
{
    assert(!filePath.empty());
    const auto& loadConfigs = onlineConfig.loadConfigList.GetLoadConfigs();
    for (const auto& loadConfig : loadConfigs)
    {
        if (loadConfig.Match(filePath, lifecycle))
        {
            return loadConfig;
        }
    }
    return onlineConfig.loadConfigList.GetDefaultLoadConfig();
}

bool DeployIndexWrapper::GetTruncateMetaDeployFiles(
        FileList& fileList, int64_t &totalLength)
{
    if (!FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(
                            mNewPath, TRUNCATE_META_DIR_NAME)))
    {
        return true;
    }
    fileList.push_back(string(TRUNCATE_META_DIR_NAME) + "/");
    return GetFiles(TRUNCATE_META_DIR_NAME, fileList, totalLength);
}

bool DeployIndexWrapper::GetAdaptiveBitmapMetaDeployFiles(
        FileList& fileList, int64_t &totalLength)
{
    if (!FileSystemWrapper::IsExist(FileSystemWrapper::JoinPath(
                            mNewPath, ADAPTIVE_DICT_DIR_NAME)))
    {
        return true;
    }
    fileList.push_back(string(ADAPTIVE_DICT_DIR_NAME) + "/");
    return GetFiles(ADAPTIVE_DICT_DIR_NAME, fileList, totalLength);
}

bool DeployIndexWrapper::GetSegmentDeployFiles(
        const Version& version,
        const vector<segmentid_t>& segIds,
        FileList& fileList, int64_t &totalLength)
{
    for (size_t i = 0; i < segIds.size(); ++i)
    {
        if (!GetFiles(version.GetSegmentDirName(segIds[i]), fileList, totalLength))
        {
            return false;
        }
    }
    return true;
}

bool DeployIndexWrapper::GetPatchSegmentDeployFiles(
        const Version& newVersion, const versionid_t lastVersionId,
        const vector<segmentid_t>& segIds, FileList& fileList, int64_t &totalLength)
{
    if (newVersion.GetSchemaVersionId() == DEFAULT_SCHEMAID)
    {
        return true;
    }

    fileList.push_back(PartitionPatchMeta::GetPatchMetaFileName(newVersion.GetVersionId()));
    PartitionPatchMeta patchMeta;
    patchMeta.Load(mNewPath, newVersion.GetVersionId());

    schemavid_t lastVersionSchemaId = DEFAULT_SCHEMAID;
    if (lastVersionId != INVALID_VERSION)
    {
        // not first deploy scene
        stringstream lss;
        lss << VERSION_FILE_NAME_PREFIX << "." << lastVersionId;
        string lastVersionPath = FileSystemWrapper::JoinPath(mLastPath, lss.str());
        Version lastVersion;
        try
        {
            VersionLoader::GetVersion(lastVersionPath, lastVersion);
        }
        catch (const exception &ex)
        {
            IE_LOG(ERROR, "get version [%s]: %s", lastVersionPath.c_str(), ex.what());
        }
        lastVersionSchemaId = lastVersion.GetSchemaVersionId();
        if (lastVersion.GetVersionId() == INVALID_VERSION ||
            lastVersionSchemaId != newVersion.GetSchemaVersionId())
        {
            fileList.push_back(newVersion.GetSchemaFileName());
        }
    }

    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext())
    {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++)
        {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (schemaInfo->GetSchemaId() > lastVersionSchemaId ||
                find(segIds.begin(), segIds.end(), segMeta.GetSegmentId()) != segIds.end())
            {
                string relativePath = FileSystemWrapper::JoinPath(
                        PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                        newVersion.GetSegmentDirName(segMeta.GetSegmentId()));
                if (!GetFiles(relativePath, fileList, totalLength))
                {
                    return false;
                }
            }
        }
    }
    return true;
}

bool DeployIndexWrapper::GetDiffSegmentIds(
        vector<segmentid_t>& segIds,
        const Version& newVersion,
        const versionid_t newVersionId,
        const versionid_t lastVersionId)
{
    if (newVersionId <= lastVersionId)
    {
        IE_LOG(ERROR, "new version [%d] is not newer than lastVersion [%d]!",
               newVersionId, lastVersionId);
        return false;
    }

    if (lastVersionId == INVALID_VERSION)
    {
        Version::Iterator iter = newVersion.CreateIterator();
        while (iter.HasNext())
        {
            segIds.push_back(iter.Next());
        }
        return true;
    }

    stringstream lss;
    lss << VERSION_FILE_NAME_PREFIX << "." << lastVersionId;
    string lastVersionPath = FileSystemWrapper::JoinPath(
            mLastPath, lss.str());

    Version lastVersion;
    if (!FileSystemWrapper::IsExist(lastVersionPath))
    {
        IE_LOG(WARN, "version file [%s] not exist!",
               lastVersionPath.c_str());
    }
    else
    {
        try
        {
            VersionLoader::GetVersion(lastVersionPath, lastVersion);
        }
        catch (const exception &ex)
        {
            IE_LOG(ERROR, "get version [%s]: %s", lastVersionPath.c_str(), ex.what());
            return false;
        }
    }

    Version::Iterator iter = newVersion.CreateIterator();
    while (iter.HasNext())
    {
        segmentid_t segId = iter.Next();
        if (!lastVersion.HasSegment(segId))
        {
            segIds.push_back(segId);
        }
    }
    return true;
}

string DeployIndexWrapper::GetDeployMetaFileName(versionid_t versionId)
{
    stringstream ss;
    ss << DEPLOY_META_FILE_NAME_PREFIX << "." << versionId;
    return ss.str();
}

string DeployIndexWrapper::GetDeployMetaLockFileName(versionid_t versionId)
{
    return DeployIndexWrapper::GetDeployMetaFileName(versionId) + DEPLOY_META_LOCK_SUFFIX;
}

void DeployIndexWrapper::DumpDeployMeta(const string& partitionPath,
        const Version& version)
{
    DirectoryPtr partitionDirectory = DirectoryCreator::Create(partitionPath);
    return DumpDeployMeta(partitionDirectory, version);
}

void DeployIndexWrapper::DumpDeployMeta(
        const DirectoryPtr& partitionDirectory, const Version& version)
{
    IndexFileList finalIndexFileList;
    FillIndexFileList(partitionDirectory, version, finalIndexFileList);
    string deployMetaFileName = GetDeployMetaFileName(version.GetVersionId());
    finalIndexFileList.Append(FileInfo(deployMetaFileName, -1, (uint64_t)-1));
    if (partitionDirectory->IsExist(deployMetaFileName)) {
        partitionDirectory->RemoveFile(deployMetaFileName);
    }
    partitionDirectory->Store(deployMetaFileName, finalIndexFileList.ToString(), true);
}

void DeployIndexWrapper::FillIndexFileList(
    const DirectoryPtr& partitionDirectory, const Version& version,
    IndexFileList& finalIndexFileList)
{
    FillBaseIndexFileList(partitionDirectory, version, finalIndexFileList);
    FillSegmentIndexFileList(partitionDirectory, version, finalIndexFileList);
    finalIndexFileList.AppendFinal(FileInfo(
            version.GetVersionFileName(), version.ToString().size(), (uint64_t)-1));
}

void DeployIndexWrapper::FillBaseIndexFileList(
        const DirectoryPtr& partitionDirectory,
        const Version& version, IndexFileList& finalIndexFileList)
{
    assert(finalIndexFileList.deployFileMetas.empty());

    FillOneIndexFileList(partitionDirectory,
                         INDEX_FORMAT_VERSION_FILE_NAME, finalIndexFileList);
    FillOneIndexFileList(partitionDirectory,
                         version.GetSchemaFileName(), finalIndexFileList);
    FillOneIndexFileList(partitionDirectory,
                         INDEX_PARTITION_META_FILE_NAME, finalIndexFileList);

    if (version.GetSchemaVersionId() != DEFAULT_SCHEMAID)
    {
        FillOneIndexFileList(partitionDirectory,
                             PartitionPatchMeta::GetPatchMetaFileName(version.GetVersionId()),
                             finalIndexFileList);
    }

    // TODO: check schema
    if (partitionDirectory->IsExist(TRUNCATE_META_DIR_NAME))
    {
        MergeIndexFileList(partitionDirectory, TRUNCATE_META_DIR_NAME,
                           finalIndexFileList);
    }

    if (partitionDirectory->IsExist(ADAPTIVE_DICT_DIR_NAME))
    {
        MergeIndexFileList(partitionDirectory, ADAPTIVE_DICT_DIR_NAME,
                           finalIndexFileList);
    }
}

void DeployIndexWrapper::FillOneIndexFileList(
        const DirectoryPtr& partitionDirectory, const string& fileName,
        IndexFileList& finalIndexFileList)
{
    fslib::FileMeta fileMeta;
    try
    {
        partitionDirectory->GetFileMeta(fileName, fileMeta);
    }
    catch (const NonExistException& e)
    {
        return;
    }
    catch (...)
    {
        throw;
    }
    finalIndexFileList.Append(FileInfo(
                    fileName, fileMeta.fileLength, fileMeta.lastModifyTime));
}

void DeployIndexWrapper::MergeIndexFileList(
        const DirectoryPtr& partitionDirectory, const string& dirName,
        IndexFileList& finalIndexFileList)
{
    string deployIndexStr;
    IndexFileList deployIndexMeta;
    if (!SegmentFileListWrapper::Load(
            partitionDirectory->GetDirectory(dirName, false), deployIndexMeta))
    {
        INDEXLIB_FATAL_ERROR(FileIO, "Load segment file list [%s] failed", dirName.c_str());
        return;
    }
    
    for (auto& meta : deployIndexMeta.deployFileMetas)
    {
        meta.filePath = FileSystemWrapper::JoinPath(dirName, meta.filePath);
        finalIndexFileList.Append(meta);
    }

    string path = FileSystemWrapper::JoinPath(dirName, SEGMENT_FILE_LIST);
    string legacyPath = FileSystemWrapper::JoinPath(dirName, DEPLOY_INDEX_FILE_NAME);
    FillOneIndexFileList(partitionDirectory, path, finalIndexFileList);
    FillOneIndexFileList(partitionDirectory, legacyPath, finalIndexFileList);
    finalIndexFileList.Append(dirName + "/");
}

void DeployIndexWrapper::FillSegmentIndexFileList(
        const DirectoryPtr& partitionDirectory, const Version& version,
        IndexFileList& finalIndexFileList)
{
    Version::Iterator iter = version.CreateIterator();
    while (iter.HasNext())
    {
        segmentid_t segmentId = iter.Next();
        string segmentDirName = version.GetSegmentDirName(segmentId);
        MergeIndexFileList(partitionDirectory, segmentDirName, finalIndexFileList);
    }

    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID)
    {
        return;
    }
    PartitionPatchMeta patchMeta;
    patchMeta.Load(partitionDirectory, version.GetVersionId());
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext())
    {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++)
        {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (!version.HasSegment(segMeta.GetSegmentId()))
            {
                continue;
            }

            string patchSegPath = FileSystemWrapper::JoinPath(
                    PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                    version.GetSegmentDirName(segMeta.GetSegmentId()));

            IE_LOG(INFO, "fill deploy index file by patch index segment [%s]", 
                   patchSegPath.c_str());
            MergeIndexFileList(partitionDirectory, patchSegPath, finalIndexFileList);
        }
    }
}

bool DeployIndexWrapper::GetDeployIndexMeta(IndexFileList& deployIndexMeta,
        const versionid_t newVersion, const versionid_t lastVersion, bool needComplete)
{
    try
    {
        return DoGetDeployIndexMeta(deployIndexMeta, newVersion, lastVersion, needComplete);
    }
    catch (const exception &ex)
    {
        IE_LOG(ERROR, "GetDeployIndexMeta fail:%s", ex.what());
        return false;
    }
    catch (...)
    {
        IE_LOG(ERROR, "GetDeployIndexMeta fail, unknown exception");
        return false;
    }
}

bool DeployIndexWrapper::DoGetDeployIndexMeta(IndexFileList& deployIndexMeta,
        const versionid_t newVersionId, const versionid_t lastVersionId, bool needComplete)
{
    string newIndexFileListPath = FileSystemWrapper::JoinPath(
            mNewPath, DeployIndexWrapper::GetDeployMetaFileName(newVersionId));
    string lastIndexFileListPath = FileSystemWrapper::JoinPath(
            mLastPath, DeployIndexWrapper::GetDeployMetaFileName(lastVersionId));

    // for compatible: old index and new searcher
    if (!FileSystemWrapper::IsExist(newIndexFileListPath)
        || (lastVersionId != INVALID_VERSION && !FileSystemWrapper::IsExist(lastIndexFileListPath)))
    {
        IE_LOG(INFO, "new binary read old index, newVersion[%d], lastVersion[%d]",
               newVersionId, lastVersionId);
        deployIndexMeta.isComplete = false;
        int64_t totalLength = 0;
        FileList fileList;
        if (!DoGetDeployFiles(fileList, totalLength, newVersionId, lastVersionId))
        {
            return false;
        }
        for (const auto& file : fileList)
        {
            deployIndexMeta.Append(FileInfo(file));
        }
        deployIndexMeta.AppendFinal(FileInfo(
                        Version::GetVersionFileName(newVersionId)));
        return true;
    }

    if (lastVersionId == INVALID_VERSION)
    {
        // first deploy
        if (!deployIndexMeta.Load(newIndexFileListPath)) {
            return false;
        }
    }
    else
    {    
        // diff
        IndexFileList newIndexFileList;
        IndexFileList lastIndexFileList;
        if (!newIndexFileList.Load(newIndexFileListPath) ||
            !lastIndexFileList.Load(lastIndexFileListPath)) {
            return false;
        }
        deployIndexMeta.FromDifference(newIndexFileList, lastIndexFileList);
    }
    deployIndexMeta.Filter([this, &deployIndexMeta](const FileInfo& fileInfo)
                           {
                               return !NeedDeploy(fileInfo.filePath, deployIndexMeta.lifecycle);
                           });

    if (needComplete)
    {
        CompleteIndexFileList(mNewPath, deployIndexMeta);
    }
    return true;
}

bool DeployIndexWrapper::GetDeployIndexMeta(
    DeployIndexMeta& remoteDeployIndexMeta, DeployIndexMeta& localDeployIndexMeta,
    const string& sourcePath, const string& localPath,
    versionid_t targetVersionId, versionid_t baseVersionId,
    const OnlineConfig& targetOnlineConfig, const OnlineConfig& baseOnlineConfig) noexcept
{
    try
    {
        return DoGetDeployIndexMeta(remoteDeployIndexMeta, localDeployIndexMeta,
                                    sourcePath, localPath,
                                    targetVersionId, baseVersionId,
                                    targetOnlineConfig, baseOnlineConfig);
    }
    catch (const exception &ex)
    {
        IE_LOG(ERROR, "GetDeployIndexMeta fail: base [%s/version.%d], target [%s/version.%d]. exception [%s]", localPath.c_str(), baseVersionId, sourcePath.c_str(), targetVersionId, ex.what());
        return false;
    }
    catch (...)
    {
        IE_LOG(ERROR, "GetDeployIndexMeta fail: base [%s/version.%d], target [%s/version.%d]. unknown exception", localPath.c_str(), baseVersionId, sourcePath.c_str(), targetVersionId);
        return false;
    }
}

bool DeployIndexWrapper::DoGetDeployIndexMeta(
    DeployIndexMeta& remoteDeployIndexMeta, DeployIndexMeta& localDeployIndexMeta,
    const string& sourcePath, const string& localPath,
    versionid_t targetVersionId, versionid_t baseVersionId,
    const OnlineConfig& targetOnlineConfig, const OnlineConfig& baseOnlineConfig)
{
    IndexFileList baseIndexFileList;
    string baseDeployMetaName = GetDeployMetaFileName(baseVersionId);
    bool localExist = false;
    bool sourceExist = false;
    string localMetaPath = PathUtil::JoinPath(localPath, baseDeployMetaName);
    string sourceMetaPath = PathUtil::JoinPath(sourcePath, baseDeployMetaName);
    LoadConfig baseFilterConfig;
    GenerateLoadConfigFromVersion(localPath, baseVersionId, baseFilterConfig);
    if (baseVersionId != INVALID_VERSION &&
        !baseIndexFileList.Load(localMetaPath, &localExist) &&
        !baseIndexFileList.Load(sourceMetaPath, &sourceExist))
    {
        if (localExist || sourceExist)
        {
            IE_LOG(ERROR, "base version deploy meta [%s] in [%s] [%s] exist but load fail",
                   baseDeployMetaName.c_str(), localPath.c_str(), sourcePath.c_str());
            return false;
        }
        IE_LOG(WARN, "base version deploy meta [%s] in [%s] [%s] not exist, construct now",
               baseDeployMetaName.c_str(), localPath.c_str(), sourcePath.c_str());
        Version baseVersion;
        auto fs = IndexlibFileSystemCreator::Create(localPath, sourcePath, true);
        DirectoryPtr dir = DirectoryCreator::Create(fs, localPath);
        VersionLoader::GetVersion(dir, baseVersion, baseVersionId);
        FillIndexFileList(dir, baseVersion, baseIndexFileList);
    }

    IndexFileList targetIndexFileList;
    string targetDeployMetaPath = PathUtil::JoinPath(
        sourcePath, GetDeployMetaFileName(targetVersionId));
    LoadConfig targetFilterConfig;
    GenerateLoadConfigFromVersion(sourcePath, targetVersionId, targetFilterConfig);
    bool targetExist = false;
    if (!targetIndexFileList.Load(targetDeployMetaPath, &targetExist))
    {
        if (targetExist)
        {
            IE_LOG(ERROR, "target version deploy meta [%s] exist but load failed",
                   targetDeployMetaPath.c_str());
            return false;
        }
        IE_LOG(WARN, "target version deploy meta [%s] not exist, construct now",
               targetDeployMetaPath.c_str());
        DirectoryPtr dir = DirectoryCreator::Create(sourcePath);
        Version targetVersion;
        VersionLoader::GetVersion(dir, targetVersion, targetVersionId);
        FillIndexFileList(dir, targetVersion, targetIndexFileList);
    }
    PickFiles(remoteDeployIndexMeta, localDeployIndexMeta,
              targetIndexFileList, baseIndexFileList,
              targetOnlineConfig, baseOnlineConfig,
              targetFilterConfig, baseFilterConfig);
    CompleteIndexFileList(sourcePath, localDeployIndexMeta);
    CompleteIndexFileList(sourcePath, remoteDeployIndexMeta);
    return true;
}

void DeployIndexWrapper::PickFiles(
    DeployIndexMeta& remoteDeployIndexMeta, DeployIndexMeta& localDeployIndexMeta,
    const IndexFileList& targetIndexFileList, const IndexFileList& baseIndexFileList,
    const OnlineConfig& targetOnlineConfig, const OnlineConfig& baseOnlineConfig,
    const LoadConfig& targetFilterConfig, const LoadConfig& baseFilterConfig)
{
    set<string> baseIndexFilePaths;
    for (const FileInfo& fileInfo : baseIndexFileList.deployFileMetas)
    {
        baseIndexFilePaths.insert(fileInfo.filePath);
    }
    for (const FileInfo& fileInfo : baseIndexFileList.finalDeployFileMetas)
    {
        baseIndexFilePaths.insert(fileInfo.filePath);
    }
    auto DoPickFiles = [&](bool isFinal, const IndexFileList::FileInfoVec& fileInfos) {
        for (const FileInfo& fileInfo : fileInfos)
        {
            const string& filePath = fileInfo.filePath;
            const LoadConfig& baseLoadConfig = baseFilterConfig.Match(filePath, 
                    baseIndexFileList.lifecycle) ? baseFilterConfig : 
            GetLoadConfig(baseOnlineConfig, filePath,  baseIndexFileList.lifecycle);
            const LoadConfig& targetLoadConfig = targetFilterConfig.Match(filePath, 
                    targetIndexFileList.lifecycle) ? targetFilterConfig : 
            GetLoadConfig(targetOnlineConfig, filePath, targetIndexFileList.lifecycle);
            if (targetLoadConfig.NeedDeploy())
            {
                if (baseIndexFilePaths.count(filePath) == 0 || !baseLoadConfig.NeedDeploy())
                {
                    localDeployIndexMeta.Append(fileInfo, isFinal);
                }
            }
            if (targetLoadConfig.IsRemote())
            {
                if (baseIndexFilePaths.count(filePath) == 0 || !baseLoadConfig.IsRemote())
                {
                    remoteDeployIndexMeta.Append(fileInfo, isFinal);
                }
            }
        }
    };
    DoPickFiles(true, targetIndexFileList.finalDeployFileMetas);
    DoPickFiles(false, targetIndexFileList.deployFileMetas);
}

void DeployIndexWrapper::CompleteIndexFileList(const string& rootPath,
        IndexFileList& deployIndexMeta)
{
    for (auto& deployFileMeta : deployIndexMeta.deployFileMetas)
    {
        if (!deployFileMeta.isValid())
        {
            string filePath = FileSystemWrapper::JoinPath(rootPath, deployFileMeta.filePath);
            fslib::FileMeta fileMeta;
            FileSystemWrapper::GetFileMeta(filePath, fileMeta);
            deployFileMeta.fileLength = fileMeta.fileLength;
            deployFileMeta.modifyTime = fileMeta.lastModifyTime;
        }
        if (deployFileMeta.modifyTime == (uint64_t)-1)
        {
            deployFileMeta.modifyTime = (uint64_t)TimeUtility::currentTimeInSeconds();
        }
    }
    deployIndexMeta.isComplete = true;
}

bool DeployIndexWrapper::GenerateLoadConfigFromVersion(const string& indexRoot,
                                                       versionid_t versionId,
                                                       LoadConfig& loadConfig)
{
    string versionPath = FileSystemWrapper::JoinPath(
        indexRoot, Version::GetVersionFileName(versionId));
    Version version;
    IndexPartitionSchemaPtr schema;
    try
    {
        VersionLoader::GetVersion(versionPath, version);
        schema = SchemaAdapter::LoadSchema(indexRoot, version.GetSchemaVersionId());
    }
    catch (...)
    {
        IE_LOG(ERROR, "load version[%s] or schema failed", versionPath.c_str());
        return false;
    }

    for (auto &id : version.GetOngoingModifyOperations())
    {
        if (!schema->MarkOngoingModifyOperation(id))
        {
            IE_LOG(ERROR, "mark ongoing modify operation [%d] fail!", id);
            return false;
        }
    }
    if (unlikely(!schema)) {
        IE_LOG(ERROR, "version[%s] schema is empty", versionPath.c_str());
        return false;
    }
    
    set<string> indexNames;
    const auto& indexSchema = schema->GetIndexSchema();
    if (indexSchema) {
        IndexConfigIteratorPtr indexConfigs =
            indexSchema->CreateIterator(true, CIT_NOT_NORMAL);
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++)
        {
            const string& indexName = (*iter)->GetIndexName();
            if ((*iter)->GetStatus() == IndexStatus::is_deleted &&
                indexSchema->GetIndexConfig(indexName))
            {
                // index/attribute is deleted and added again, need to deploy this index/attribute
                continue;
            }
            indexNames.insert(indexName);
        }
    }
    set<string> attrNames;
    set<string> packAttrNames;
    const auto& attrSchema = schema->GetAttributeSchema();
    if (attrSchema) {
        AttributeConfigIteratorPtr attrConfigs =
            attrSchema->CreateIterator(CIT_NOT_NORMAL);
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
        {
            const string& attrName = (*iter)->GetAttrName();
            if ((*iter)->GetStatus() == IndexStatus::is_deleted &&
                attrSchema->GetAttributeConfig(attrName))
        {
            // index/attribute is deleted and added again, need to deploy this index/attribute
            continue;
        }
            attrNames.insert(attrName);
        }
        PackAttributeConfigIteratorPtr packConfigs =
            attrSchema->CreatePackAttrIterator(CIT_DISABLE);
        for (auto iter = packConfigs->Begin(); iter != packConfigs->End(); iter++)
        {
            // pack attribute not support delete
            packAttrNames.insert((*iter)->GetAttrName());
        }
    }

    loadConfig = OnlineConfig::CreateDisableLoadConfig(
            NOT_READY_OR_DELETE_FIELDS_CONFIG_NAME, indexNames, attrNames, packAttrNames);
    return true;
}


IE_NAMESPACE_END(index_base);

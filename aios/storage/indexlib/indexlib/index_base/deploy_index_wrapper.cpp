/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index_base/deploy_index_wrapper.h"

#include <time.h>

#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/disable_fields_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IndexFileDeployer.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/lifecycle_table_construct.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace fslib;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::index;

namespace indexlib { namespace index_base {
IE_DECLARE_AND_SETUP_LOGGER(DeployIndexWrapper);

bool DeployIndexWrapper::DumpTruncateMetaIndex(const file_system::DirectoryPtr& dir)
{
    time_t t_start = time(NULL);
    file_system::DirectoryPtr truncateMetaDir = dir->GetDirectory(TRUNCATE_META_DIR_NAME, false);
    if (truncateMetaDir != nullptr) {
        if (truncateMetaDir->IsExist(SEGMENT_FILE_LIST) && truncateMetaDir->IsExist(DEPLOY_INDEX_FILE_NAME)) {
            return true;
        }
        bool result = SegmentFileListWrapper::Dump(truncateMetaDir, "");
        truncateMetaDir->Close();
        time_t t_end = time(NULL);
        IE_LOG(INFO, "Deploy truncate index time [%lf]", difftime(t_end, t_start));
        return result;
    }
    return true;
}

bool DeployIndexWrapper::DumpAdaptiveBitmapMetaIndex(const file_system::DirectoryPtr& dir)
{
    time_t t_start = time(NULL);
    file_system::DirectoryPtr adaptiveDictDir = dir->GetDirectory(ADAPTIVE_DICT_DIR_NAME, false);
    if (adaptiveDictDir != nullptr) {
        if (adaptiveDictDir->IsExist(SEGMENT_FILE_LIST) && adaptiveDictDir->IsExist(DEPLOY_INDEX_FILE_NAME)) {
            return true;
        }
        bool result = SegmentFileListWrapper::Dump(adaptiveDictDir, "");
        adaptiveDictDir->Close();
        time_t t_end = time(NULL);
        IE_LOG(INFO, "Deploy adaptive bitmap index time [%lf]", difftime(t_end, t_start));
        return result;
    }
    return true;
}

bool DeployIndexWrapper::TEST_DumpSegmentDeployIndex(const std::string& segmentPath, const fslib::FileList& fileList)
{
    time_t t_start = time(NULL);
    bool result = SegmentFileListWrapper::TEST_Dump(segmentPath, fileList);
    time_t t_end = time(NULL);
    IE_LOG(INFO, "Deploy segment[%s] index time [%lf]", segmentPath.c_str(), difftime(t_end, t_start));
    return result;
}

bool DeployIndexWrapper::DumpSegmentDeployIndex(const file_system::DirectoryPtr& segDir, const string& lifecycle)
{
    time_t t_start = time(NULL);
    bool result = SegmentFileListWrapper::Dump(segDir, lifecycle);
    time_t t_end = time(NULL);
    IE_LOG(INFO, "Deploy segment[%s] index time [%lf]", segDir->DebugString().c_str(), difftime(t_end, t_start));
    return result;
}

bool DeployIndexWrapper::DumpSegmentDeployIndex(const file_system::DirectoryPtr& segDir,
                                                const fslib::FileList& fileList)
{
    time_t t_start = time(NULL);
    bool result = SegmentFileListWrapper::Dump(segDir, fileList);
    time_t t_end = time(NULL);
    IE_LOG(INFO, "Deploy segment[%s] index time [%lf]", segDir->DebugString().c_str(), difftime(t_end, t_start));
    return result;
}

void DeployIndexWrapper::DumpSegmentDeployIndex(const DirectoryPtr& directory, const SegmentInfoPtr& segmentInfo)
{
    assert(directory);
    SegmentFileListWrapper::Dump(directory, segmentInfo);
}

int64_t DeployIndexWrapper::GetIndexSize(const std::string& rawPath, const versionid_t targetVersion)
{
    try {
        GetDeployIndexMetaInputParams inputParams;
        inputParams.rawPath = rawPath;
        inputParams.targetVersionId = targetVersion;
        DeployIndexMetaVec localDeployIndexMetaVec;
        DeployIndexMetaVec remoteDeployIndexMetaVec;
        if (!GetDeployIndexMeta(inputParams, localDeployIndexMetaVec, remoteDeployIndexMetaVec)) {
            return -1;
        }

        auto sumFunc = [](const IndexFileList::FileInfoVec& fileInfos) {
            int64_t totalLength = 0;
            for (const auto& fileInfo : fileInfos) {
                totalLength += (fileInfo.fileLength != -1) ? fileInfo.fileLength : 0;
            }
            return totalLength;
        };

        int64_t totalLength = 0;
        for (const auto& deployIndexMeta : localDeployIndexMetaVec) {
            totalLength += sumFunc(deployIndexMeta->deployFileMetas);
            totalLength += sumFunc(deployIndexMeta->finalDeployFileMetas);
        }
        for (const auto& deployIndexMeta : remoteDeployIndexMetaVec) {
            totalLength += sumFunc(deployIndexMeta->deployFileMetas);
            totalLength += sumFunc(deployIndexMeta->finalDeployFileMetas);
        }
        return totalLength;
    } catch (const exception& ex) {
        IE_LOG(ERROR, "GetDeployIndexMeta fail:%s", ex.what());
    } catch (...) {
        IE_LOG(ERROR, "GetDeployIndexMeta fail, unknown exception");
    }
    return -1;
}

bool DeployIndexWrapper::TEST_GetSegmentSize(const SegmentData& segmentData, bool includePatch, int64_t& totalLength)
{
    totalLength = GetSegmentSize(segmentData.GetDirectory(), includePatch);
    if (totalLength < 0) {
        return false;
    }

    const PartitionPatchIndexAccessorPtr& patchAccessor = segmentData.GetPatchIndexAccessor();
    if (!patchAccessor) {
        return true;
    }
    const PartitionPatchMeta& patchMeta = patchAccessor->GetPartitionPatchMeta();
    DirectoryPtr rootDir = patchAccessor->GetRootDirectory();
    const Version& version = patchAccessor->GetVersion();
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext()) {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (segmentData.GetSegmentId() != segMeta.GetSegmentId()) {
                continue;
            }
            string patchPath = PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId());
            file_system::DirectoryPtr patchDir = rootDir->GetDirectory(patchPath, false);
            if (patchDir == nullptr) {
                IE_LOG(ERROR, "get directory failed, empty dir[%s/%s]", rootDir->DebugString().c_str(),
                       patchPath.c_str());
                return false;
            }
            string patchSegPath = version.GetSegmentDirName(segMeta.GetSegmentId());
            file_system::DirectoryPtr patchSegDir = patchDir->GetDirectory(patchSegPath, false);
            if (patchSegDir == nullptr) {
                IE_LOG(ERROR, "get directory failed, empty dir[%s/%s]", patchDir->DebugString().c_str(),
                       patchSegPath.c_str());
                return false;
            }

            IE_LOG(INFO, "target segment [%d] in [%s/%s]", segmentData.GetSegmentId(), patchDir->DebugString().c_str(),
                   patchSegPath.c_str());
            int64_t patchLen = GetSegmentSize(patchSegDir, includePatch);
            if (patchLen < 0) {
                return false;
            }
            totalLength += patchLen;
        }
    }
    return true;
}

int64_t DeployIndexWrapper::GetSegmentSize(const file_system::DirectoryPtr& directory, bool includePatch)
{
    IndexFileList dpIndexMeta;
    if (!SegmentFileListWrapper::Load(directory, dpIndexMeta)) {
        return -1;
    }

    int64_t totalLength = 0;
    for (size_t i = 0; i < dpIndexMeta.deployFileMetas.size(); ++i) {
        if (!includePatch && index_base::PatchFileFinder::IsAttrPatchFile(
                                 util::PathUtil::GetFileName(dpIndexMeta.deployFileMetas[i].filePath))) {
            continue;
        }

        if (dpIndexMeta.deployFileMetas[i].fileLength > 0) {
            totalLength += dpIndexMeta.deployFileMetas[i].fileLength;
        }
    }
    return totalLength;
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

void DeployIndexWrapper::DumpDeployMeta(const string& partitionPath, const Version& version)
{
    auto fs = FileSystemCreator::Create("DumpDeployMeta", partitionPath, FileSystemOptions::Offline()).GetOrThrow();
    return DumpDeployMeta(Directory::Get(fs), version);
}

static void FillOneIndexFileList(const DirectoryPtr& partitionDirectory, const string& fileName,
                                 IndexFileList& finalIndexFileList)
{
    try {
        auto len = partitionDirectory->GetFileLength(fileName);
        finalIndexFileList.Append(FileInfo(fileName, len));
    } catch (const NonExistException& e) {
        return;
    } catch (...) {
        throw;
    }
}

static void MergeIndexFileList(const DirectoryPtr& partitionDirectory, const string& dirName,
                               IndexFileList& finalIndexFileList)
{
    string deployIndexStr;
    IndexFileList deployIndexMeta;
    if (!SegmentFileListWrapper::Load(partitionDirectory->GetDirectory(dirName, false), deployIndexMeta)) {
        INDEXLIB_FATAL_ERROR(FileIO, "Load segment file list [%s] failed", dirName.c_str());
        return;
    }

    for (auto& meta : deployIndexMeta.deployFileMetas) {
        meta.filePath = util::PathUtil::JoinPath(dirName, meta.filePath);
        finalIndexFileList.Append(meta);
    }

    string path = util::PathUtil::JoinPath(dirName, SEGMENT_FILE_LIST);
    string legacyPath = util::PathUtil::JoinPath(dirName, DEPLOY_INDEX_FILE_NAME);
    FillOneIndexFileList(partitionDirectory, path, finalIndexFileList);
    FillOneIndexFileList(partitionDirectory, legacyPath, finalIndexFileList);
    finalIndexFileList.Append(dirName + "/");
}

static void FillBaseIndexFileList(const DirectoryPtr& partitionDirectory, const Version& version,
                                  IndexFileList& finalIndexFileList)
{
    assert(finalIndexFileList.deployFileMetas.empty());

    FillOneIndexFileList(partitionDirectory, INDEX_FORMAT_VERSION_FILE_NAME, finalIndexFileList);
    FillOneIndexFileList(partitionDirectory, version.GetSchemaFileName(), finalIndexFileList);
    FillOneIndexFileList(partitionDirectory, INDEX_PARTITION_META_FILE_NAME, finalIndexFileList);

    if (version.GetSchemaVersionId() != DEFAULT_SCHEMAID) {
        FillOneIndexFileList(partitionDirectory, PartitionPatchMeta::GetPatchMetaFileName(version.GetVersionId()),
                             finalIndexFileList);
    }

    // TODO: check schema
    if (partitionDirectory->IsExist(TRUNCATE_META_DIR_NAME)) {
        MergeIndexFileList(partitionDirectory, TRUNCATE_META_DIR_NAME, finalIndexFileList);
    }

    if (partitionDirectory->IsExist(ADAPTIVE_DICT_DIR_NAME)) {
        MergeIndexFileList(partitionDirectory, ADAPTIVE_DICT_DIR_NAME, finalIndexFileList);
    }
}

static void FillSegmentIndexFileList(const DirectoryPtr& partitionDirectory, const Version& version,
                                     IndexFileList& finalIndexFileList)
{
    Version::Iterator iter = version.CreateIterator();
    while (iter.HasNext()) {
        segmentid_t segmentId = iter.Next();
        string segmentDirName = version.GetSegmentDirName(segmentId);
        MergeIndexFileList(partitionDirectory, segmentDirName, finalIndexFileList);
    }

    if (version.GetSchemaVersionId() == DEFAULT_SCHEMAID) {
        return;
    }

    bool exist = false;
    const auto dirPath = partitionDirectory->GetOutputPath();
    const auto schemFile = dirPath + '/' + version.GetSchemaFileName();
    if (FslibWrapper::IsExist(schemFile, exist) == FSEC_OK && !exist) {
        IE_LOG(ERROR, "schema file[%s] not exist", schemFile.c_str());
        return;
    }
    PartitionPatchMeta patchMeta;
    patchMeta.Load(partitionDirectory, version.GetVersionId());
    PartitionPatchMeta::Iterator metaIter = patchMeta.CreateIterator();
    while (metaIter.HasNext()) {
        SchemaPatchInfoPtr schemaInfo = metaIter.Next();
        assert(schemaInfo);
        SchemaPatchInfo::Iterator sIter = schemaInfo->Begin();
        for (; sIter != schemaInfo->End(); sIter++) {
            const SchemaPatchInfo::PatchSegmentMeta& segMeta = *sIter;
            if (!version.HasSegment(segMeta.GetSegmentId())) {
                continue;
            }

            string patchSegPath =
                util::PathUtil::JoinPath(PartitionPatchIndexAccessor::GetPatchRootDirName(schemaInfo->GetSchemaId()),
                                         version.GetSegmentDirName(segMeta.GetSegmentId()));

            IE_LOG(INFO, "fill deploy index file by patch index segment [%s]", patchSegPath.c_str());
            MergeIndexFileList(partitionDirectory, patchSegPath, finalIndexFileList);
        }
    }
}

static void FillIndexFileList(const DirectoryPtr& partitionDirectory, const Version& version,
                              IndexFileList& finalIndexFileList)
{
    FillBaseIndexFileList(partitionDirectory, version, finalIndexFileList);
    FillSegmentIndexFileList(partitionDirectory, version, finalIndexFileList);
    finalIndexFileList.AppendFinal(FileInfo(version.GetVersionFileName(), version.ToString().size(), (uint64_t)-1));
}

void DeployIndexWrapper::DumpDeployMeta(const DirectoryPtr& partitionDirectory, const Version& version)
{
    IndexFileList finalIndexFileList;
    FillIndexFileList(partitionDirectory, version, finalIndexFileList);
    string deployMetaFileName = GetDeployMetaFileName(version.GetVersionId());
    finalIndexFileList.Append(FileInfo(deployMetaFileName, -1, (uint64_t)-1));
    if (partitionDirectory->IsExist(deployMetaFileName)) {
        partitionDirectory->RemoveFile(deployMetaFileName);
        // deploy_meta.x maybe mount read-only
        const std::string dpMetaPath = PathUtil::JoinPath(partitionDirectory->GetOutputPath(), deployMetaFileName);
        auto ec = file_system::FslibWrapper::DeleteFile(
                      dpMetaPath, DeleteOption::Fence(partitionDirectory->GetFenceContext().get(), true))
                      .Code();
        THROW_IF_FS_ERROR(ec, "Delete file failed, path[%s].", dpMetaPath.c_str());
    }
    partitionDirectory->Store(deployMetaFileName, finalIndexFileList.ToString(), WriterOption::AtomicDump());
}

bool DeployIndexWrapper::GenerateDisableLoadConfig(const string& rootPath, versionid_t versionId,
                                                   LoadConfig& loadConfig)
{
    if (versionId == INVALID_VERSION) {
        auto ret = IndexFileDeployer::GetLastVersionId(rootPath);
        if (!ret.OK()) {
            return false;
        }
        versionId = ret.result;
    }

    std::string versionPath = Version::GetVersionFileName(versionId);
    Version version;
    IndexPartitionSchemaPtr schema;
    try {
        VersionLoader::GetVersionS(rootPath, version, versionId);
        schema = IndexPartitionSchema::Load(PathUtil::JoinPath(rootPath, version.GetSchemaFileName()));
    } catch (...) {
        IE_LOG(WARN, "load version[%s] or schema failed", versionPath.c_str());
        return false;
    }

    if (unlikely(!schema)) {
        IE_LOG(WARN, "version[%s] schema is empty", versionPath.c_str());
        return false;
    }

    for (auto& id : version.GetOngoingModifyOperations()) {
        if (!schema->MarkOngoingModifyOperation(id)) {
            IE_LOG(ERROR, "mark ongoing modify operation [%d] fail!", id);
            return false;
        }
    }

    set<string> indexNames;
    const auto& indexSchema = schema->GetIndexSchema();
    if (indexSchema) {
        IndexConfigIteratorPtr indexConfigs = indexSchema->CreateIterator(true, is_disable_or_deleted);
        for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++) {
            const string& indexName = (*iter)->GetIndexName();
            if ((*iter)->GetStatus() == IndexStatus::is_deleted && indexSchema->GetIndexConfig(indexName)) {
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
        AttributeConfigIteratorPtr attrConfigs = attrSchema->CreateIterator(is_disable_or_deleted);
        for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++) {
            const string& attrName = (*iter)->GetAttrName();
            if ((*iter)->GetStatus() == IndexStatus::is_deleted && attrSchema->GetAttributeConfig(attrName)) {
                // index/attribute is deleted and added again, need to deploy this index/attribute
                continue;
            }
            attrNames.insert(attrName);
        }
        PackAttributeConfigIteratorPtr packConfigs = attrSchema->CreatePackAttrIterator(is_disable);
        for (auto iter = packConfigs->Begin(); iter != packConfigs->End(); iter++) {
            // pack attribute not support delete
            packAttrNames.insert((*iter)->GetPackName());
        }
    }
    bool disableSummaryField = false;
    const auto& summarySchema = schema->GetSummarySchema();
    if (summarySchema && summarySchema->IsAllFieldsDisabled()) {
        disableSummaryField = true;
    }

    bool disableSourceField = false;
    const auto& sourceSchema = schema->GetSourceSchema();
    if (sourceSchema && sourceSchema->IsAllFieldsDisabled()) {
        disableSourceField = true;
    }

    vector<groupid_t> disableGroupIds;
    if (sourceSchema) {
        sourceSchema->GetDisableGroupIds(disableGroupIds);
    }
    // __NOT_READY_OR_DELETE_FIELDS__
    loadConfig =
        OnlineConfig::CreateDisableLoadConfig(DISABLE_FIELDS_LOAD_CONFIG_NAME, indexNames, attrNames, packAttrNames,
                                              disableSummaryField, disableSourceField, disableGroupIds);
    return true;
}

static bool FillDeployIndexMeta(const string& rootPath, versionid_t versionId, const LoadConfigList& loadConfigList,
                                const std::shared_ptr<LifecycleTable>& lifeCycleTable,
                                DeployIndexMetaVec* localDeployIndexMetaVec,
                                DeployIndexMetaVec* remoteDeployIndexMetaVec)
{
    assert(localDeployIndexMetaVec);
    assert(remoteDeployIndexMetaVec);

    IndexFileDeployer indexFileDeployer(localDeployIndexMetaVec, remoteDeployIndexMetaVec);
    file_system::ErrorCode ec =
        indexFileDeployer.FillDeployIndexMetaVec(versionId, rootPath, loadConfigList, lifeCycleTable);
    if (ec == FSEC_OK) {
        IE_LOG(INFO, "fill deploy index meta, root[%s], version[%d], local[%lu], remote[%lu]", rootPath.c_str(),
               versionId, localDeployIndexMetaVec->size(), remoteDeployIndexMetaVec->size());
        return true;
    } else if (ec == FSEC_NOENT) {
        IE_LOG(WARN, "fill deploy index meta failed, root[%s], version[%d], local[%lu], remote[%lu]", rootPath.c_str(),
               versionId, localDeployIndexMetaVec->size(), remoteDeployIndexMetaVec->size());
        return false;
    }
    IE_LOG(ERROR, "fill deploy index meta failed, root[%s], versionId[%d], ec[%d]", rootPath.c_str(), versionId, ec);
    return false;
}

static bool IsDeployMetaExist(const string& rootPath, versionid_t versionId)
{
    FSResult<bool> ret = IndexFileDeployer::IsDeployMetaExist(rootPath, versionId);
    if (ret.OK()) {
        return ret.GetOrThrow();
    }
    // treat io error as not exist
    IE_LOG(ERROR, "check version exists failed, root[%s], versionId[%d], ec[%d]", rootPath.c_str(), versionId, ret.ec);
    return false;
}

static bool DifferenceDeployIndexMeta(DeployIndexMetaVec& baseDeployIndexMetaVec,
                                      DeployIndexMetaVec& targetDeployIndexMetaVec,
                                      DeployIndexMetaVec* resultDeployIndexMetaVec)
{
    assert(resultDeployIndexMetaVec);

    // targetRootPath -> DeployIndexMeta
    std::map<std::string, DeployIndexMeta*> baseDeployIndexMetaMap;
    for (auto& curDeployIndexMeta : baseDeployIndexMetaVec) {
        auto ret = baseDeployIndexMetaMap.insert({curDeployIndexMeta->targetRootPath, curDeployIndexMeta.get()});
        assert(ret.second);
        if (!ret.second) {
            IE_LOG(ERROR, "duplicate root[%s]", curDeployIndexMeta->targetRootPath.c_str());
            return false;
        }
    }
    // resultDeployIndexMetaVec = targetDeployIndexMetaVec - baseDeployIndexMetaVec
    for (auto& curDeployIndexMeta : targetDeployIndexMetaVec) {
        auto iter = baseDeployIndexMetaMap.find(curDeployIndexMeta->targetRootPath);
        if (iter == baseDeployIndexMetaMap.end()) {
            resultDeployIndexMetaVec->emplace_back(std::move(curDeployIndexMeta));
            continue;
        }
        resultDeployIndexMetaVec->emplace_back(new DeployIndexMeta());
        auto& lastDeployIndexMeta = resultDeployIndexMetaVec->back();
        lastDeployIndexMeta->targetRootPath = curDeployIndexMeta->targetRootPath;
        lastDeployIndexMeta->sourceRootPath = curDeployIndexMeta->sourceRootPath;
        lastDeployIndexMeta->FromDifference(*curDeployIndexMeta, *iter->second);
    }
    return true;
}

static LoadConfigList RewriteLoadConfigList(const string& rootPath, const config::OnlineConfig& onlineConfig,
                                            versionid_t versionId, const string& localPath, const string& remotePath)
{
    LoadConfigList loadConfigList = onlineConfig.loadConfigList;

    loadConfigList.SetDefaultRootPath(localPath, remotePath);

    LoadConfig disabeLoadConfig;
    bool ret = DeployIndexWrapper::GenerateDisableLoadConfig(rootPath, versionId, disabeLoadConfig);
    (void)ret; // for UT, see GenerateLoadConfigFromVersion from old commit
    loadConfigList.PushFront(disabeLoadConfig);
    return loadConfigList;
}

bool DeployIndexWrapper::GetDeployIndexMeta(const GetDeployIndexMetaInputParams& inputParams,
                                            DeployIndexMetaVec& localDeployIndexMetaVec,
                                            DeployIndexMetaVec& remoteDeployIndexMetaVec) noexcept
{
    const std::string& rawPath = PathUtil::NormalizePath(inputParams.rawPath);
    const std::string& localPath =
        PathUtil::NormalizePath(inputParams.localPath.empty() ? inputParams.rawPath : inputParams.localPath);
    const std::string& remotePath = PathUtil::NormalizePath(inputParams.remotePath);
    const config::OnlineConfig& baseOnlineConfig =
        inputParams.baseOnlineConfig ? *inputParams.baseOnlineConfig : config::OnlineConfig();
    const config::OnlineConfig& targetOnlineConfig =
        inputParams.targetOnlineConfig ? *inputParams.targetOnlineConfig : config::OnlineConfig();
    versionid_t baseVersionId = inputParams.baseVersionId;
    versionid_t targetVersionId = inputParams.targetVersionId;

    localDeployIndexMetaVec.clear();
    remoteDeployIndexMetaVec.clear();

    // get target DeployIndexMetaVec
    // TODO: opt with config when no deploy
    DeployIndexMetaVec targetLocalDeployIndexMetaVec;
    DeployIndexMetaVec targetRemoteDeployIndexMetaVec;
    LoadConfigList targetLoadConfigList =
        RewriteLoadConfigList(rawPath, targetOnlineConfig, targetVersionId, localPath, remotePath);

    indexlib::file_system::LifecycleTablePtr lifecycle;
    if (targetOnlineConfig.NeedLoadWithLifeCycle()) {
        lifecycle.reset(new file_system::LifecycleTable());
        if (!LifecycleTableConstruct::ConstructLifeCycleTable(rawPath, targetVersionId, lifecycle)) {
            IE_LOG(ERROR, "failed to construct life cycle table for version [%d], path [%s]", targetVersionId,
                   rawPath.c_str());
            return false;
        }
    }

    if (!FillDeployIndexMeta(rawPath, targetVersionId, targetLoadConfigList, lifecycle, &targetLocalDeployIndexMetaVec,
                             &targetRemoteDeployIndexMetaVec)) {
        IE_LOG(ERROR, "get target deploy index meta [%d] failed, raw[%s], local[%s]", targetVersionId, rawPath.c_str(),
               localPath.c_str());
        return false;
    }
    if (baseVersionId == INVALID_VERSION) {
        localDeployIndexMetaVec = std::move(targetLocalDeployIndexMetaVec);
        remoteDeployIndexMetaVec = std::move(targetRemoteDeployIndexMetaVec);
        return true;
    }

    // get base DeployIndexMetaVec
    DeployIndexMetaVec baseLocalDeployIndexMetaVec;
    DeployIndexMetaVec baseRemoteDeployIndexMetaVec;
    bool getFromLocalSuccess = false;
    if (baseOnlineConfig.NeedLoadWithLifeCycle()) {
        lifecycle.reset(new file_system::LifecycleTable());
        if (!LifecycleTableConstruct::ConstructLifeCycleTable(rawPath, baseVersionId, lifecycle)) {
            IE_LOG(ERROR, "failed to construct life cycle table for version [%d], path [%s]", baseVersionId,
                   rawPath.c_str());
            return false;
        }
    } else {
        lifecycle.reset();
    }

    if (IsDeployMetaExist(localPath, baseVersionId)) {
        // localPath first
        LoadConfigList baseLoadConfigList =
            RewriteLoadConfigList(localPath, baseOnlineConfig, baseVersionId, localPath, remotePath);
        getFromLocalSuccess = FillDeployIndexMeta(localPath, baseVersionId, baseLoadConfigList, lifecycle,
                                                  &baseLocalDeployIndexMetaVec, &baseRemoteDeployIndexMetaVec);
    }
    if (!getFromLocalSuccess) {
        // try rawPath
        LoadConfigList baseLoadConfigList =
            RewriteLoadConfigList(rawPath, baseOnlineConfig, baseVersionId, localPath, remotePath);
        if (!FillDeployIndexMeta(rawPath, baseVersionId, baseLoadConfigList, lifecycle, &baseLocalDeployIndexMetaVec,
                                 &baseRemoteDeployIndexMetaVec)) {
            IE_LOG(WARN, "get base deploy index meta [%d] failed, raw[%s], local[%s], deploy all", baseVersionId,
                   rawPath.c_str(), localPath.c_str());
            localDeployIndexMetaVec = std::move(targetLocalDeployIndexMetaVec);
            remoteDeployIndexMetaVec = std::move(targetRemoteDeployIndexMetaVec);
            return true;
        }
    }

    // return (target - base)
    if (!DifferenceDeployIndexMeta(baseLocalDeployIndexMetaVec, targetLocalDeployIndexMetaVec,
                                   &localDeployIndexMetaVec)) {
        IE_LOG(ERROR, "diff local deploy index meta failed, path[%s -> %s], version[%d -> %d]", localPath.c_str(),
               rawPath.c_str(), baseVersionId, targetVersionId);
        return false;
    }
    if (!DifferenceDeployIndexMeta(baseRemoteDeployIndexMetaVec, targetRemoteDeployIndexMetaVec,
                                   &remoteDeployIndexMetaVec)) {
        IE_LOG(ERROR, "diff remote deploy index meta failed, path[%s -> %s], version[%d -> %d]", localPath.c_str(),
               rawPath.c_str(), baseVersionId, targetVersionId);
        return false;
    }
    return true;
}

bool DeployIndexWrapper::GenerateDisableLoadConfig(const indexlibv2::config::TabletOptions& tabletOptions,
                                                   file_system::LoadConfig& loadConfig)
{
    config::DisableFieldsConfig disableFieldsConfig;
    if (auto status = tabletOptions.GetFromRawJson("online_index_config.disable_fields", &disableFieldsConfig);
        !status.IsOKOrNotFound()) {
        IE_LOG(ERROR, "get config from tablet options failed, status[%s]", status.ToString().c_str());
    } else if (!status.IsOK()) {
        return false;
    }
    if (!disableFieldsConfig.rewriteLoadConfig) {
        return false;
    }

    if (disableFieldsConfig.attributes.size() > 0 || disableFieldsConfig.packAttributes.size() > 0 ||
        disableFieldsConfig.indexes.size() > 0 || disableFieldsConfig.summarys != DisableFieldsConfig::SDF_FIELD_NONE ||
        disableFieldsConfig.sources != DisableFieldsConfig::CDF_FIELD_NONE) {
        loadConfig = OnlineConfig::CreateDisableLoadConfig(
            DISABLE_FIELDS_LOAD_CONFIG_NAME, disableFieldsConfig.indexes, disableFieldsConfig.attributes,
            disableFieldsConfig.packAttributes, disableFieldsConfig.summarys != DisableFieldsConfig::SDF_FIELD_NONE,
            disableFieldsConfig.sources == DisableFieldsConfig::CDF_FIELD_ALL,
            disableFieldsConfig.disabledSourceGroupIds);
    }
    return true;
}

}} // namespace indexlib::index_base

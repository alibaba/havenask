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
#include "indexlib/file_system/EntryTableBuilder.h"

#include <algorithm>
#include <assert.h>
#include <map>
#include <string.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/EntryTableJsonizable.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/file_system/LegacyVersion.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/VersionedPackageFileMeta.h"
#include "indexlib/util/PathUtil.h"
using indexlib::util::PathUtil;
using std::string;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, EntryTableBuilder);

EntryTableBuilder::EntryTableBuilder() {}

EntryTableBuilder::~EntryTableBuilder() {}

std::shared_ptr<EntryTable> EntryTableBuilder::CreateEntryTable(const string& name, const string& outputRoot,
                                                                const std::shared_ptr<FileSystemOptions>& fsOptions)
{
    _entryTable.reset(new EntryTable(name, outputRoot, fsOptions->isOffline));
    _entryTable->Init();
    _options = fsOptions;
    const auto& loadConfigList = _options->loadConfigList;
    if (loadConfigList.NeedLoadWithLifeCycle()) {
        _lifecycleTable.reset(new LifecycleTable());
    }
    return _entryTable;
}

ErrorCode EntryTableBuilder::MountFile(const string& physicalRoot, const string& physicalPath,
                                       const string& logicalPath, int64_t length, FSMountType mountType)
{
    if (unlikely(
            autil::StringUtil::startsWith(PathUtil::GetFileName(physicalPath), ENTRY_TABLE_FILE_NAME_DOT_PREFIX))) {
        return FSEC_OK;
    }
    const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
    bool isOwner = mountType == FSMT_READ_WRITE;
    return DoMountFile(pPhysicalRoot, physicalPath, logicalPath, length, isOwner, ConflictResolution::OVERWRITE);
}
ErrorCode EntryTableBuilder::DoMountFile(const string* pPhysicalRoot, const string& physicalPath,
                                         const string& logicalPath, int64_t length, bool isOwner,
                                         ConflictResolution conflictResolution)
{
    string physicalFileName = PathUtil::GetFileName(physicalPath);
    auto IsPackagePair = PackageFileMeta::IsPackageFileName(physicalFileName);
    if (IsPackagePair.first) {
        if (IsPackagePair.second && !VersionedPackageFileMeta::IsValidFileName(physicalFileName)) {
            return MountPackageFile(pPhysicalRoot, physicalPath, logicalPath, isOwner);
        }
        // do nothing for package data file
    }
    assert(!autil::StringUtil::startsWith(physicalFileName, ENTRY_TABLE_FILE_NAME_DOT_PREFIX));

    if (length < 0) {
        string filePath = PathUtil::JoinPath(*pPhysicalRoot, physicalPath);
        ErrorCode ec = FslibWrapper::GetFileLength(filePath, length).Code();
        if (ec == FSEC_NOENT) {
            return FSEC_OK;
        }
        if (ec != FSEC_OK) {
            AUTIL_LOG(INFO, "get file length for file [%s] failed", filePath.c_str());
            return ec;
        }
    }

    assert(logicalPath == PathUtil::NormalizePath(logicalPath));
    assert(!logicalPath.empty());

    string logicalDirPath = PathUtil::GetParentDirPath(logicalPath);
    if (!logicalDirPath.empty()) {
        MountDir(pPhysicalRoot, PathUtil::GetParentDirPath(physicalPath), logicalDirPath, isOwner, conflictResolution);
    }
    MountNormalFile(pPhysicalRoot, physicalPath, logicalPath, length, isOwner, ConflictResolution::OVERWRITE);
    return FSEC_OK;
}

ErrorCode EntryTableBuilder::SetDirLifecycle(const string& rawPath, const string& lifecycle)
{
    if (_lifecycleTable == nullptr) {
        return FSEC_OK;
    }
    if (_lifecycleTable->AddDirectory(rawPath, lifecycle)) {
        return FSEC_OK;
    }
    return FSEC_ERROR;
}

void EntryTableBuilder::MountDir(const string* pPhysicalRoot, const string& physicalPath, const string& logicalPath,
                                 bool isOwner, ConflictResolution conflictResolution)
{
    assert(!logicalPath.empty());
    return MountNormalDir(pPhysicalRoot, physicalPath, logicalPath, isOwner, conflictResolution);
}

ErrorCode EntryTableBuilder::MountVersion(const string& physicalRoot, versionid_t versionId, const string& logicalPath,
                                          MountOption mountOption)
{
    const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
    bool isOwner = mountOption.mountType == FSMT_READ_WRITE;
    return DoMountVersion(pPhysicalRoot, versionId, logicalPath, isOwner, mountOption.conflictResolution);
}

ErrorCode EntryTableBuilder::DoMountVersion(const string* pPhysicalRoot, versionid_t versionId,
                                            const string& logicalPath, bool isOwner,
                                            ConflictResolution conflictResolution)
{
    if (!logicalPath.empty()) {
        MountDir(pPhysicalRoot, "", logicalPath, isOwner, ConflictResolution::OVERWRITE);
    }

    if (versionId == INVALID_VERSIONID) {
        ErrorCode ec = MountFromPreloadEntryTable(pPhysicalRoot, logicalPath, isOwner);
        if (ec != FSEC_OK) {
            AUTIL_LOG(ERROR, "mount preload_entry_table failed, physicalRoot[%s], logicalPath[%s], ec[%d]",
                      pPhysicalRoot->c_str(), logicalPath.c_str(), ec);
            return ec;
        }
        return FSEC_OK;
    }

    string versionFileName = VERSION_FILE_NAME_PREFIX + string(".") + std::to_string(versionId);
    ErrorCode ec = MountFromEntryTable(pPhysicalRoot, versionId, logicalPath, isOwner, conflictResolution);
    if (ec == FSEC_OK) {
        return VerifyMayNotExistFile(PathUtil::JoinPath(logicalPath, versionFileName));
    } else if (ec != FSEC_NOENT) {
        AUTIL_LOG(WARN, "mount entry_table[%d] failed, physicalRoot[%s], logicalPath[%s], ec[%d]", versionId,
                  pPhysicalRoot->c_str(), logicalPath.c_str(), ec);
        return ec;
    } else if (!_options->enableBackwardCompatible) {
        AUTIL_LOG(
            ERROR,
            "not enable backward compatible, mount entry_table[%d] failed, physicalRoot[%s], logicalPath[%s], ec[%d]",
            versionId, pPhysicalRoot->c_str(), logicalPath.c_str(), ec);
        return ec;
    }
    AUTIL_LOG(INFO, "mount entry_table[%d] failed, try deploy_meta, physicalRoot[%s], logicalPath[%s]", versionId,
              pPhysicalRoot->c_str(), logicalPath.c_str());

    ec = MountFromDeployMeta(pPhysicalRoot, versionId, logicalPath, isOwner);
    if (ec == FSEC_OK) {
        return VerifyMayNotExistFile(PathUtil::JoinPath(logicalPath, versionFileName));
    } else if (ec != FSEC_NOENT) {
        AUTIL_LOG(WARN, "mount deploy_meta[%d] failed, pPhysicalRoot[%s], logicalPath[%s], ec[%d]", versionId,
                  pPhysicalRoot->c_str(), logicalPath.c_str(), ec);
        return ec;
    }
    AUTIL_LOG(INFO, "mount deploy_meta[%d] failed, try version, physicalRoot[%s], logicalPath[%s]", versionId,
              pPhysicalRoot->c_str(), logicalPath.c_str());

    ec = MountFromVersion(pPhysicalRoot, versionId, logicalPath, isOwner);
    if (ec == FSEC_OK) {
        return FSEC_OK;
    }
    AUTIL_LOG(WARN, "mount version[%d] failed, physicalRoot[%s], logicalPath[%s], ec[%d]", versionId,
              pPhysicalRoot->c_str(), logicalPath.c_str(), ec);
    return ec;
}

ErrorCode EntryTableBuilder::VerifyMayNotExistFile(const std::string& logicalPath)
{
    FSResult<EntryMeta> entryMetaRet = _entryTable->GetEntryMeta(logicalPath);
    if (entryMetaRet.ec != FSEC_OK) {
        return FSEC_OK;
    }
    const EntryMeta& entryMeta = entryMetaRet.result;
    FSResult<bool> isExistRet = FslibWrapper::IsExist(entryMeta.GetFullPhysicalPath());
    if (isExistRet.ec != FSEC_OK) {
        AUTIL_LOG(WARN, "Verify file [%s] exist failed [%s]", logicalPath.c_str(),
                  entryMeta.GetFullPhysicalPath().c_str());
        return isExistRet.ec;
    }
    if (!isExistRet.result) {
        AUTIL_LOG(INFO, "Verify file [%s] not exist yet", entryMeta.GetFullPhysicalPath().c_str());
        _entryTable->Delete(logicalPath);
    }
    return FSEC_OK;
}

ErrorCode EntryTableBuilder::MountFromPreloadEntryTable(const string* pPhysicalRoot, const string& logicalPath,
                                                        bool isOwner)
{
    for (const string& entryTableFileName : {ENTRY_TABLE_PRELOAD_FILE_NAME, ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME}) {
        auto ret = FslibWrapper::IsExist(PathUtil::JoinPath(*pPhysicalRoot, entryTableFileName));
        if (!ret.OK()) {
            return ret.ec;
        }
        if (ret.result) {
            AUTIL_LOG(INFO, "find preload entry_table, physicalRoot[%s], logicalPath[%s], entryTableFileName[%s]",
                      pPhysicalRoot->c_str(), logicalPath.c_str(), entryTableFileName.c_str());
            string entryTablePath = PathUtil::JoinPath(*pPhysicalRoot, entryTableFileName);
            string jsonStr;
            auto ec = FslibWrapper::Load(entryTablePath, jsonStr).Code();
            if (unlikely(ec != FSEC_OK)) {
                return ec;
            }
            ec = FromEntryTableString(jsonStr, logicalPath, *pPhysicalRoot, isOwner, ConflictResolution::OVERWRITE);
            if (ec == FSEC_OK) {
                _lastEntryTableEntryMeta.reset(new EntryMeta(entryTableFileName, entryTableFileName, pPhysicalRoot));
            } else {
                AUTIL_LOG(ERROR, "from json string failed, path[%s], ec[%d]", entryTablePath.c_str(), ec);
            }
            return ec;
        }
    }
    AUTIL_LOG(INFO, "not found preload entry_table, physicalRoot[%s], logicalPath[%s], do nothing",
              pPhysicalRoot->c_str(), logicalPath.c_str());
    return FSEC_OK;
}

ErrorCode EntryTableBuilder::MountFromEntryTable(const string* pPhysicalRoot, versionid_t versionId,
                                                 const string& logicalPath, bool isOwner,
                                                 ConflictResolution conflictResolution)
{
    string entryTableFileName = EntryTable::GetEntryTableFileName(versionId);

    string entryTablePath = PathUtil::JoinPath(*pPhysicalRoot, entryTableFileName);
    string jsonStr;
    auto ec = FslibWrapper::Load(entryTablePath, jsonStr).Code();
    if (unlikely(ec != FSEC_OK)) {
        return ec;
    }
    ec = FromEntryTableString(jsonStr, logicalPath, *pPhysicalRoot, isOwner, conflictResolution);
    if (ec == FSEC_OK) {
        _lastEntryTableEntryMeta.reset(new EntryMeta(entryTableFileName, entryTableFileName, pPhysicalRoot));
    } else {
        AUTIL_LOG(WARN, "from json string failed, path[%s], ec[%d]", entryTablePath.c_str(), ec);
    }
    return ec;
}

ErrorCode EntryTableBuilder::MountFromDeployMeta(const string* pPhysicalRoot, versionid_t versionId,
                                                 const string& logicalPath, bool isOwner)
{
    // for compatible deploy_meta, index files and deploy_meta has same root
    string deployMetaFileName = DEPLOY_META_FILE_NAME_PREFIX + string(".") + std::to_string(versionId);
    return MountIndexFileList(pPhysicalRoot, deployMetaFileName, logicalPath, isOwner);
}

ErrorCode EntryTableBuilder::MountFromVersion(const string* pPhysicalRoot, versionid_t versionId,
                                              const string& logicalPath, bool isOwner)
{
    // load version
    string versionFileName = VERSION_FILE_NAME_PREFIX + string(".") + std::to_string(versionId);
    auto versionFilePath = PathUtil::JoinPath(*pPhysicalRoot, versionFileName);
    LegacyVersion version;
    auto ret = JsonUtil::Load(versionFilePath, &version);
    if (!ret.OK()) {
        return ret.ec;
    }
    size_t vesionSize = ret.result;

    std::vector<string> segmentDirs;
    for (const segmentid_t id : version.GetSegmentVector()) {
        segmentDirs.push_back(version.GetSegmentDirName(id));
    }

    // mount partition file
    ErrorCode ec = DoMountFile(pPhysicalRoot, SCHEMA_FILE_NAME, PathUtil::JoinPath(logicalPath, SCHEMA_FILE_NAME), -1,
                               isOwner, ConflictResolution::OVERWRITE);
    if (ec != FSEC_OK && ec != FSEC_NOENT) {
        return ec;
    }
    ec = DoMountFile(pPhysicalRoot, INDEX_FORMAT_VERSION_FILE_NAME,
                     PathUtil::JoinPath(logicalPath, INDEX_FORMAT_VERSION_FILE_NAME), -1, isOwner,
                     ConflictResolution::OVERWRITE);
    if (ec != FSEC_OK && ec != FSEC_NOENT) {
        return ec;
    }
    ec = DoMountFile(pPhysicalRoot, INDEX_PARTITION_META_FILE_NAME,
                     PathUtil::JoinPath(logicalPath, INDEX_PARTITION_META_FILE_NAME), -1, isOwner,
                     ConflictResolution::OVERWRITE);
    if (ec != FSEC_OK && ec != FSEC_NOENT) {
        return ec;
    }
    ec = DoMountFile(pPhysicalRoot, versionFileName, PathUtil::JoinPath(logicalPath, versionFileName), vesionSize,
                     isOwner, ConflictResolution::OVERWRITE);
    if (ec != FSEC_OK) {
        return ec;
    }
    // mount partition dir
    ec = DoMountSegment(pPhysicalRoot, TRUNCATE_META_DIR_NAME, PathUtil::JoinPath(logicalPath, TRUNCATE_META_DIR_NAME),
                        isOwner);
    if (ec != FSEC_OK && ec != FSEC_NOENT) {
        return ec;
    }
    ec = DoMountSegment(pPhysicalRoot, ADAPTIVE_DICT_DIR_NAME, PathUtil::JoinPath(logicalPath, ADAPTIVE_DICT_DIR_NAME),
                        isOwner);
    if (ec != FSEC_OK && ec != FSEC_NOENT) {
        return ec;
    }
    // mount segment
    for (auto& segmentDir : segmentDirs) {
        ec = DoMountSegment(pPhysicalRoot, segmentDir, PathUtil::JoinPath(logicalPath, segmentDir), isOwner);
        if (ec != FSEC_OK) {
            return ec;
        }
    }

    return FSEC_OK;
}

ErrorCode EntryTableBuilder::MountSegment(const string& physicalRoot, const string& physicalPath,
                                          const string& logicalPath, FSMountType mountType)
{
    const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
    bool isOwner = mountType == FSMT_READ_WRITE;
    return DoMountSegment(pPhysicalRoot, physicalPath, logicalPath, isOwner);
}

ErrorCode EntryTableBuilder::DoMountSegment(const string* pPhysicalRoot, const string& physicalPath,
                                            const string& logicalPath, bool isOwner)
{
    string segmentFileListFilePath = PathUtil::JoinPath(physicalPath, SEGMENT_FILE_LIST);
    string deployIndexFilePath = PathUtil::JoinPath(physicalPath, DEPLOY_INDEX_FILE_NAME);
    ErrorCode ec = MountIndexFileList(pPhysicalRoot, segmentFileListFilePath, logicalPath, isOwner);
    if (ec == FSEC_OK) {
        ec = DoMountFile(pPhysicalRoot, deployIndexFilePath, PathUtil::JoinPath(logicalPath, DEPLOY_INDEX_FILE_NAME),
                         -1, isOwner, ConflictResolution::OVERWRITE);
        if (ec != FSEC_OK && ec != FSEC_NOENT) {
            return ec;
        }
        return VerifyMayNotExistFile(PathUtil::JoinPath(logicalPath, SEGMENT_INFO_FILE_NAME));
    } else if (ec != FSEC_NOENT) {
        return ec;
    }
    AUTIL_LOG(INFO, "load segment_file_list[%s] failed in [%s], try deploy_index", segmentFileListFilePath.c_str(),
              pPhysicalRoot->c_str());
    ec = MountIndexFileList(pPhysicalRoot, deployIndexFilePath, logicalPath, isOwner);
    if (ec == FSEC_OK) {
        return VerifyMayNotExistFile(PathUtil::JoinPath(logicalPath, SEGMENT_INFO_FILE_NAME));
    } else if (ec != FSEC_NOENT) {
        return ec;
    }
    AUTIL_LOG(INFO, "load segment_file_list[%s] failed in [%s], try list dir", segmentFileListFilePath.c_str(),
              pPhysicalRoot->c_str());
    // compatible for ut
    return DoMountDirRecursive(pPhysicalRoot, physicalPath, logicalPath, isOwner, ConflictResolution::OVERWRITE);
}

ErrorCode EntryTableBuilder::MountIndexFileList(const string* pPhysicalRoot, const string& physicalPath,
                                                const string& logicalPath, bool isOwner)
{
    // load deploy_meta or segment_file_list
    auto physicalIndexFileListPath = PathUtil::JoinPath(*pPhysicalRoot, physicalPath);
    IndexFileList indexFileList;
    FSResult<int64_t> ret = indexFileList.Load(physicalIndexFileListPath);
    if (!ret.OK()) {
        return ret.ec;
    }
    // mount dir before mount inner files and after check exist
    if (!logicalPath.empty()) {
        MountDir(pPhysicalRoot, PathUtil::GetParentDirPath(physicalPath), logicalPath, isOwner,
                 ConflictResolution::OVERWRITE);
    }
    int64_t fileLength = ret.result;

    // mount all entry
    auto mountFunc = [&](const std::vector<FileInfo>& fileInfos) {
        string innerPhysicalPath = PathUtil::GetParentDirPath(physicalPath);
        for (const auto& fileInfo : fileInfos) {
            if (fileInfo.isDirectory()) {
                string path = PathUtil::NormalizePath(fileInfo.filePath); // trim tailing '/'
                MountDir(pPhysicalRoot, PathUtil::JoinPath(innerPhysicalPath, path),
                         PathUtil::JoinPath(logicalPath, path), isOwner, ConflictResolution::OVERWRITE);
            } else {
                ErrorCode ec = DoMountFile(pPhysicalRoot, PathUtil::JoinPath(innerPhysicalPath, fileInfo.filePath),
                                           PathUtil::JoinPath(logicalPath, fileInfo.filePath), fileInfo.fileLength,
                                           isOwner, ConflictResolution::OVERWRITE);
                if (ec != FSEC_OK) {
                    return ec;
                }
            }
        }
        return FSEC_OK;
    };
    indexFileList.Sort();
    ErrorCode ec = mountFunc(indexFileList.deployFileMetas);
    if (ec != FSEC_OK) {
        return ec;
    }
    ec = mountFunc(indexFileList.finalDeployFileMetas);
    if (ec != FSEC_OK) {
        return ec;
    }

    // override deploy meta file length, because in deploy meta it is unknown
    string logicalIndexFileListPath = PathUtil::JoinPath(logicalPath, PathUtil::GetFileName(physicalPath));
    if (_entryTable->IsExist(logicalIndexFileListPath)) {
        _entryTable->UpdateFileSize(logicalIndexFileListPath, fileLength);
    } else {
        // for compatible for old deploy_index
        MountNormalFile(pPhysicalRoot, physicalPath, logicalIndexFileListPath, fileLength, isOwner,
                        ConflictResolution::OVERWRITE);
    }
    return FSEC_OK;
}

void EntryTableBuilder::MountNormalFile(const string* pPhysicalRoot, const string& physicalPath,
                                        const string& logicalPath, int64_t length, bool isOwner,
                                        ConflictResolution conflictResolution)
{
    assert(PathUtil::NormalizePath(physicalPath) == physicalPath);
    assert(PathUtil::NormalizePath(logicalPath) == logicalPath);
    assert(pPhysicalRoot && PathUtil::NormalizePath(*pPhysicalRoot) == *pPhysicalRoot);

    if (conflictResolution == ConflictResolution::SKIP) {
        auto result = _entryTable->GetEntryMeta(logicalPath);
        if (result.Code() == FSEC_OK) {
            // exist entry, just skip
            return;
        }
    }

    EntryMeta entryMeta(logicalPath, physicalPath, pPhysicalRoot);
    entryMeta.SetOwner(isOwner);
    entryMeta.SetMutable(false);
    entryMeta.SetLength(length);
    RewriteEntryMeta(entryMeta);
    auto ret = _entryTable->AddEntryMeta(entryMeta);
    (void)ret;
}

void EntryTableBuilder::MountNormalDir(const string* pPhysicalRoot, const string& physicalPath,
                                       const string& logicalPath, bool isOwner, ConflictResolution conflictResolution)
{
    assert(PathUtil::NormalizePath(physicalPath) == physicalPath);
    assert(PathUtil::NormalizePath(logicalPath) == logicalPath);
    assert(pPhysicalRoot && PathUtil::NormalizePath(*pPhysicalRoot) == *pPhysicalRoot);

    if (conflictResolution == ConflictResolution::SKIP) {
        auto result = _entryTable->GetEntryMeta(logicalPath);
        if (result.Code() == FSEC_OK) {
            // exist entry, just skip
            return;
        }
    }

    EntryMeta entryMeta(logicalPath, physicalPath, pPhysicalRoot);
    entryMeta.SetOwner(isOwner);
    entryMeta.SetMutable(isOwner);
    entryMeta.SetDir();
    RewriteEntryMeta(entryMeta);
    auto ret = _entryTable->AddEntryMeta(entryMeta);
    (void)ret;
}

ErrorCode EntryTableBuilder::MountPackageDir(const std::string& physicalRoot, const std::string& physicalPath,
                                             const std::string& logicalPath, FSMountType mountType)
{
    const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
    bool isOwner = mountType == FSMT_READ_WRITE;
    std::string packageMetaFileName = std::string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX;
    return MountPackageFile(pPhysicalRoot, PathUtil::JoinPath(physicalPath, packageMetaFileName),
                            PathUtil::JoinPath(logicalPath, packageMetaFileName), isOwner);
}

ErrorCode EntryTableBuilder::MountPackageFile(const string* pPhysicalRoot, const string& physicalPath,
                                              const string& logicalFilePath, bool isOwner)
{
    // pPhysicalRoot: mainse_excellent_search/generation_1610562525/partition_0_65535/
    // physicalPath: segment_0_level_0/package_file.__meta__
    // logicalFilePath: segment_0_level_0/package_file.__meta__
    assert(pPhysicalRoot);
    PackageFileMeta packageFileMeta;
    ErrorCode ec = packageFileMeta.Load(PathUtil::JoinPath(*pPhysicalRoot, physicalPath));
    if (ec == FSEC_NOENT) {
        return ec;
    } else if (ec != FSEC_OK) {
        AUTIL_LOG(WARN, "mount package file failed, physicalRoot[%s], physicalPath[%s], logicalFilePath[%s]",
                  pPhysicalRoot->c_str(), physicalPath.c_str(), logicalFilePath.c_str());
        return ec;
    }
    // package file
    const std::vector<string>& physicalFileNames = packageFileMeta.GetPhysicalFileNames();
    const std::vector<size_t>& physicalFileLengths = packageFileMeta.GetPhysicalFileLengths();
    assert(physicalFileNames.size() == physicalFileLengths.size());
    // physicalParentPath: segment_0_level_0
    string physicalParentPath = PathUtil::GetParentDirPath(physicalPath);
    for (size_t i = 0; i < physicalFileNames.size(); ++i) {
        _entryTable->SetPackageFileLength(PathUtil::JoinPath(*pPhysicalRoot, physicalParentPath, physicalFileNames[i]),
                                          physicalFileLengths[i]); // data
    }
    size_t metaStrLength;
    RETURN_IF_FS_ERROR(packageFileMeta.GetMetaStrLength(&metaStrLength), "");
    _entryTable->SetPackageFileLength(PathUtil::JoinPath(*pPhysicalRoot, physicalPath), metaStrLength); // meta
    // in package file
    for (auto it = packageFileMeta.Begin(); it != packageFileMeta.End(); ++it) {
        // it->GetFilePath: attribute/ant_instalment
        // logicalFilePath: segment_0_level_0/attribute/ant_instalment
        string logicalEntryPath = PathUtil::JoinPath(PathUtil::GetParentDirPath(logicalFilePath), it->GetFilePath());
        if (it->IsDir()) {
            const string& metaPhysicalPath = physicalPath;
            EntryMeta entryMeta(logicalEntryPath, metaPhysicalPath, pPhysicalRoot);
            entryMeta.SetOwner(isOwner);
            entryMeta.SetMutable(isOwner);
            entryMeta.SetDir();
            entryMeta.SetOffset(0);
            RewriteEntryMeta(entryMeta);
            auto ret = _entryTable->AddEntryMeta(entryMeta);
            (void)ret;
        } else {
            string dataPhysicalPath =
                PathUtil::JoinPath(physicalParentPath, packageFileMeta.GetPhysicalFileName(it->GetDataFileIdx()));
            EntryMeta entryMeta(logicalEntryPath, dataPhysicalPath, pPhysicalRoot);
            entryMeta.SetOwner(isOwner);
            entryMeta.SetMutable(false);
            entryMeta.SetLength(it->GetLength());
            entryMeta.SetOffset(it->GetOffset());
            RewriteEntryMeta(entryMeta);
            auto ret = _entryTable->AddEntryMeta(entryMeta);
            (void)ret;
        }
    }
    return FSEC_OK;
}

// Compare two physical paths and return true if left < right.
// two possible package file meta names:
// package_file.__meta__.desc.i
// package_file.__meta__
bool EntryTableBuilder::CompareFile(const string& left, const string& right)
{
    string leftDir = PathUtil::GetParentDirPath(left);
    string rightDir = PathUtil::GetParentDirPath(right);
    int res = leftDir.compare(rightDir);
    if (res != 0) {
        return res < 0;
    }
    string leftFileName = PathUtil::GetFileName(left);
    string rightFileName = PathUtil::GetFileName(right);
    bool leftIsPackage = leftFileName.find(PACKAGE_FILE_META_SUFFIX) != string::npos;
    bool rightIsPackage = rightFileName.find(PACKAGE_FILE_META_SUFFIX) != string::npos;
    if (!leftIsPackage && !rightIsPackage) {
        return left.compare(right) < 0;
    }
    if (leftIsPackage && !rightIsPackage) {
        return false;
    }
    if (!leftIsPackage && rightIsPackage) {
        return true;
    }
    bool leftIsValidVersionFile = VersionedPackageFileMeta::IsValidFileName(leftFileName);
    bool rightIsValidVersionFile = VersionedPackageFileMeta::IsValidFileName(rightFileName);
    if (leftIsValidVersionFile && rightIsValidVersionFile) {
        string leftDescription = VersionedPackageFileMeta::GetDescription(leftFileName);
        string rightDescription = VersionedPackageFileMeta::GetDescription(rightFileName);
        res = leftDescription.compare(rightDescription);
        if (res == 0) {
            return VersionedPackageFileMeta::GetVersionId(leftFileName) <
                   VersionedPackageFileMeta::GetVersionId(rightFileName);
        }
        return res < 0;
    }
    if (leftIsValidVersionFile) {
        return false;
    }
    return true;
}

ErrorCode EntryTableBuilder::MountDirRecursive(const string& physicalRoot, const string& physicalPath,
                                               const string& logicalPath, MountOption mountOption)
{
    const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
    bool isOwner = mountOption.mountType == FSMT_READ_WRITE;
    return DoMountDirRecursive(pPhysicalRoot, physicalPath, logicalPath, isOwner, mountOption.conflictResolution);
}

ErrorCode EntryTableBuilder::DoMountDirRecursive(const string* pPhysicalRoot, const string& physicalPath,
                                                 const string& logicalPath, bool isOwner,
                                                 ConflictResolution conflictResolution)
{
    // if (unlikely(autil::StringUtil::endsWith(physicalPath, FILE_SYSTEM_WAL_SUFFIX))) {
    //     return FSEC_OK;
    // }

    assert(pPhysicalRoot);
    fslib::FileList fileList;
    string physicalFullPath = PathUtil::JoinPath(*pPhysicalRoot, physicalPath);
    // TODO: opt with list rich
    ErrorCode ec = FslibWrapper::ListDirRecursive(physicalFullPath, fileList).Code();
    if (ec != FSEC_OK) {
        if (ec == FSEC_NOENT) {
            AUTIL_LOG(INFO, "path does not exist. pPhysicalRoot[%s], physicalPath[%s], logicalPath[%s]",
                      pPhysicalRoot->c_str(), physicalPath.c_str(), logicalPath.c_str());
            return FSEC_NOENT;
        }
        AUTIL_LOG(WARN, "list path failed. pPhysicalRoot[%s], physi calPath[%s], logicalPath[%s], ec[%d]",
                  pPhysicalRoot->c_str(), physicalPath.c_str(), logicalPath.c_str(), ec);
        return ec;
    }
    if (!logicalPath.empty()) {
        MountDir(pPhysicalRoot, physicalPath, logicalPath, isOwner, conflictResolution);
    }

    std::sort(fileList.begin(), fileList.end(), CompareFile);
    for (const auto& path : fileList) {
        bool isDir = *path.rbegin() == '/';
        string normPath = PathUtil::NormalizePath(path); // trim tailing '/'
        // if (unlikely(autil::StringUtil::endsWith(normPath, FILE_SYSTEM_WAL_SUFFIX))) {
        //     continue;
        // }
        if (isDir) {
            MountDir(pPhysicalRoot, PathUtil::JoinPath(physicalPath, normPath),
                     PathUtil::JoinPath(logicalPath, normPath), isOwner, conflictResolution);
        } else {
            if (unlikely(
                    autil::StringUtil::startsWith(PathUtil::GetFileName(normPath), ENTRY_TABLE_FILE_NAME_DOT_PREFIX))) {
                continue;
            }
            ec = DoMountFile(pPhysicalRoot, PathUtil::JoinPath(physicalPath, normPath),
                             PathUtil::JoinPath(logicalPath, normPath), -1, isOwner, conflictResolution);
            if (ec != FSEC_OK) {
                return ec;
            }
        }
    }
    _entryTable->SetEntryMetaLazy(logicalPath, false);
    return FSEC_OK;
}

FSResult<versionid_t> EntryTableBuilder::GetLastVersion(const string& rootPath)
{
    FileList fileList;
    auto ec = FslibWrapper::ListDir(rootPath, fileList).Code();
    if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(WARN, "list dir [%s] failed", rootPath.c_str());
        return {ec, -1};
    }
    versionid_t versionId = INVALID_VERSIONID;
    for (const string& fileName : fileList) {
        if (autil::StringUtil::startsWith(fileName, VERSION_FILE_NAME_DOT_PREFIX)) {
            auto tmpVersionId = autil::StringUtil::strToInt32WithDefault(
                fileName.substr(VERSION_FILE_NAME_DOT_PREFIX.size()).c_str(), -1);
            versionId = std::max(versionId, tmpVersionId);
        }
    }
    if (unlikely(versionId == INVALID_VERSIONID)) {
        AUTIL_LOG(INFO, "no version or entry_table in dir [%s]", rootPath.c_str());
        return {FSEC_NOENT, INVALID_VERSIONID};
    }
    return {FSEC_OK, versionId};
}

static const LoadConfig& MatchLoadConfig(const string& logicalPath, const std::shared_ptr<FileSystemOptions>& options,
                                         const LifecycleTablePtr& lifecycleTable)
{
    const auto& loadConfigList = options->loadConfigList;
    for (size_t i = 0; i < loadConfigList.Size(); ++i) {
        const auto& loadConfig = loadConfigList.GetLoadConfig(i);
        const auto& lifecycle = lifecycleTable == nullptr ? "" : lifecycleTable->GetLifecycle(logicalPath);
        if (loadConfig.Match(logicalPath, lifecycle)) {
            return loadConfig;
        }
    }
    return loadConfigList.GetDefaultLoadConfig();
}

// refer to IndexFileDeployer::FillByOneEntryMeta
void EntryTableBuilder::RewriteEntryMeta(EntryMeta& entryMeta) const
{
    if (!_options->redirectPhysicalRoot) {
        return;
    }
    assert(!_options->isOffline);

    if (autil::StringUtil::startsWith(entryMeta.GetLogicalPath(), RT_INDEX_PARTITION_DIR_NAME) ||
        autil::StringUtil::startsWith(entryMeta.GetLogicalPath(), JOIN_INDEX_PARTITION_DIR_NAME) ||
        autil::StringUtil::endsWith(entryMeta.GetPhysicalRoot(), RT_INDEX_PARTITION_DIR_NAME)) {
        return;
    }

    // online partition need redirect physical root from raw dfs to local fs (DP) or remote dfs(dcache or mpangu or
    // pangu)
    const LoadConfig& loadConfig = MatchLoadConfig(entryMeta.GetLogicalPath(), _options, _lifecycleTable);
    auto [physicalRoot, fenceName] = indexlibv2::PathUtil::ExtractFenceName(entryMeta.GetPhysicalRoot());
    if (loadConfig.IsRemote()) {
        // for online partiton remote
        const string& remoteRoot = PathUtil::JoinPath(loadConfig.GetRemoteRootPath(), fenceName);
        if (entryMeta.GetPhysicalRoot() != remoteRoot) {
            AUTIL_LOG(TRACE3, "RemoteRedirect: [%s] --> [%s], [%s]", entryMeta.GetPhysicalRoot().c_str(),
                      remoteRoot.c_str(), entryMeta.GetLogicalPath().c_str());
            const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(remoteRoot);
            entryMeta.SetPhysicalRoot(pPhysicalRoot);
            _entryTable->UpdatePackageFileLengthsCache(entryMeta);
        }
        entryMeta.SetOwner(false); // we can not delete file or dir in remote, read only
    } else if (loadConfig.NeedDeploy()) {
        // for online partiton local
        const string& localRoot = PathUtil::JoinPath(loadConfig.GetLocalRootPath(), fenceName);
        if (entryMeta.GetPhysicalRoot() != localRoot) {
            AUTIL_LOG(TRACE3, "LocalRedirect: [%s] --> [%s], [%s]", entryMeta.GetPhysicalRoot().c_str(),
                      localRoot.c_str(), entryMeta.GetLogicalPath().c_str());
            const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(localRoot);
            entryMeta.SetPhysicalRoot(pPhysicalRoot);
            _entryTable->UpdatePackageFileLengthsCache(entryMeta);
        }
    } else {
        // !remote && !deploy: may be flush to remote? we do nothing
        AUTIL_LOG(TRACE3, "NoRedirect: [%s] , [%s]", entryMeta.GetPhysicalRoot().c_str(),
                  entryMeta.GetLogicalPath().c_str());
    }
}

ErrorCode EntryTableBuilder::FillFromPackageFiles(EntryTableJsonizable& json, std::vector<EntryMeta>* entryMetas,
                                                  const std::string& logicalPath, bool isOwner, bool isMutable,
                                                  const std::string& defaultPhysicalRoot)
{
    for (auto& packageFilesPair : json.GetPackageFiles()) {
        // trim tailing '/'
        string physicalRoot =
            packageFilesPair.first.empty() ? defaultPhysicalRoot : PathUtil::NormalizePath(packageFilesPair.first);
        const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
        for (auto& packagePair : packageFilesPair.second) {
            const string& physicalPath = packagePair.first;
            string physicalFullPath = PathUtil::JoinPath(physicalRoot, physicalPath);
            EntryTableJsonizable::PackageMetaInfo& packageMetaInfo = packagePair.second;
            _entryTable->SetPackageFileLength(physicalFullPath, packageMetaInfo.length);
            for (auto& entryPair : packageMetaInfo.files) {
                EntryMeta& entryMeta = entryPair.second;
                entryMeta.SetOwner(isOwner);
                if (entryMeta.IsDir()) {
                    entryMeta.SetMutable(isMutable);
                } else if (entryMeta.IsFile()) {
                    assert(entryMeta.GetLength() >= 0);
                } else {
                    AUTIL_LOG(WARN, "unknown entryMeta[%s]", entryMeta.DebugString().c_str());
                    return FSEC_ERROR;
                }
                entryMeta.SetPhysicalRoot(pPhysicalRoot);
                entryMeta.SetPhysicalPath(physicalPath);
                entryMeta.SetLogicalPath(PathUtil::JoinPath(logicalPath, entryPair.first));
                entryMetas->emplace_back(entryMeta);
            }
        }
    }
    return FSEC_OK;
}

ErrorCode EntryTableBuilder::FillFromFiles(EntryTableJsonizable& json, std::vector<EntryMeta>* entryMetas,
                                           const std::string& logicalPath, bool isOwner, bool isMutable,
                                           const std::string& defaultPhysicalRoot)
{
    for (auto& filesPair : json.GetFiles()) {
        // trim tailing '/'
        string physicalRoot = filesPair.first.empty() ? defaultPhysicalRoot : PathUtil::NormalizePath(filesPair.first);
        const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
        for (auto& entryPair : filesPair.second) {
            EntryMeta& entryMeta = entryPair.second;
            entryMeta.SetOwner(isOwner);
            if (entryMeta.IsDir()) {
                entryMeta.SetMutable(isMutable);
            } else if (entryMeta.IsFile()) {
                assert(entryMeta.GetLength() >= 0);
            } else {
                AUTIL_LOG(WARN, "unknown entryMeta[%s]", entryMeta.DebugString().c_str());
                return FSEC_ERROR;
            }
            entryMeta.SetPhysicalRoot(pPhysicalRoot);
            if (!logicalPath.empty()) {
                // empty will use logicalPath as physicalPath to save memory
                entryMeta.SetPhysicalPath(entryPair.first);
            }
            entryMeta.SetLogicalPath(PathUtil::JoinPath(logicalPath, entryPair.first));
            entryMetas->emplace_back(entryMeta);
        }
    }
    return FSEC_OK;
}

ErrorCode EntryTableBuilder::FillEntryTable(EntryTableJsonizable& json, const std::string& logicalPath,
                                            const std::string& defaultPhysicalRoot, bool isOwner,
                                            ConflictResolution conflictResolution)
{
    bool isMutable = isOwner;
    std::vector<EntryMeta> entryMetas;
    ErrorCode ec = FillFromFiles(json, &entryMetas, logicalPath, isOwner, isMutable, defaultPhysicalRoot);
    if (ec != FSEC_OK) {
        return ec;
    }
    ec = FillFromPackageFiles(json, &entryMetas, logicalPath, isOwner, isMutable, defaultPhysicalRoot);
    if (ec != FSEC_OK) {
        return ec;
    }

    // sort to make sure add dir first
    auto CompByLogicalPath = [](const EntryMeta& lhs, const EntryMeta& rhs) {
        return lhs.GetLogicalPath() < rhs.GetLogicalPath();
    };
    std::sort(entryMetas.begin(), entryMetas.end(), CompByLogicalPath);

    for (EntryMeta& entryMeta : entryMetas) {
        RewriteEntryMeta(entryMeta);
        if (conflictResolution == ConflictResolution::CHECK_DIFF) {
            auto result = _entryTable->GetEntryMeta(entryMeta.GetLogicalPath());
            if (result.OK()) {
                const EntryMeta& currentEntryMeta = result.result;
                if (currentEntryMeta.GetFullPhysicalPath() != entryMeta.GetFullPhysicalPath()) {
                    AUTIL_LOG(ERROR,
                              "entry meta logical path[%s] physical path[%s] not equal to current exist file "
                              "physical "
                              "path[%s]. "
                              "fill entry table failed.",
                              entryMeta.GetLogicalPath().c_str(), entryMeta.GetFullPhysicalPath().c_str(),
                              currentEntryMeta.GetFullPhysicalPath().c_str());
                    return FSEC_BADARGS;
                }
            }
        } else if (conflictResolution == ConflictResolution::SKIP) {
            auto result = _entryTable->GetEntryMeta(logicalPath);
            if (result.OK()) {
                // exist entry, just skip
                continue;
            }
        }

        ErrorCode ec = _entryTable->AddEntryMeta(entryMeta).ec;
        if (ec != FSEC_OK) {
            return ec;
        }
    }
    return FSEC_OK;
}

ErrorCode EntryTableBuilder::FromEntryTableString(const string& jsonStr, const string& logicalPath,
                                                  const string& defaultPhysicalRoot, bool isOwner,
                                                  ConflictResolution conflictResolution)
{
    EntryTableJsonizable json;
    RETURN_IF_FS_EXCEPTION(autil::legacy::FromJsonString(json, jsonStr), "from json string failed, path[%s]",
                           defaultPhysicalRoot.c_str());
    return FillEntryTable(json, logicalPath, defaultPhysicalRoot, isOwner, conflictResolution);
}

ErrorCode EntryTableBuilder::TEST_FromEntryTableString(const string& jsonStr, const string& defaultPhysicalRoot)
{
    return FromEntryTableString(jsonStr, /*logicalPath*/ "", defaultPhysicalRoot, /*isOwner=*/false,
                                ConflictResolution::OVERWRITE);
}

const EntryMeta* EntryTableBuilder::GetLastEntryTableEntryMeta() const { return _lastEntryTableEntryMeta.get(); }

ErrorCode EntryTableBuilder::MountDirLazy(const std::string& physicalRoot, const std::string& physicalPath,
                                          const std::string& logicalPath, MountOption mountOption)
{
    assert(mountOption.conflictResolution == ConflictResolution::OVERWRITE);

    const string* pPhysicalRoot = _entryTable->GetPhysicalRootPointer(physicalRoot);
    EntryMeta entryMeta(logicalPath, physicalPath, pPhysicalRoot);
    entryMeta.SetLazy(true);
    entryMeta.SetOwner(true);
    entryMeta.SetMutable(true);
    entryMeta.SetDir();
    RewriteEntryMeta(entryMeta);
    return _entryTable->AddEntryMeta(entryMeta).ec;
}
}} // namespace indexlib::file_system

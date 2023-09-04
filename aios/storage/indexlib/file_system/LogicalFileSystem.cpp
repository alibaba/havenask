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
#include "indexlib/file_system/LogicalFileSystem.h"

#include <algorithm>
#include <assert.h>
#include <ext/alloc_traits.h>
#include <ostream>
#include <unordered_set>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/package/DirectoryMerger.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LogicalFileSystem);

class StorageMetrics;

LogicalFileSystem::LogicalFileSystem(const std::string& name, const std::string& outputRoot,
                                     util::MetricProviderPtr metricProvider) noexcept
    : _lock(new autil::RecursiveThreadMutex())
    , _name(name)
    , _outputRoot(PathUtil::NormalizePath(outputRoot))
    , _entryTableBuilder(new EntryTableBuilder())
{
    AUTIL_LOG(INFO, "Create LogicalFS [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
    _metricsReporter.reset(new FileSystemMetricsReporter(metricProvider));
}

LogicalFileSystem::~LogicalFileSystem() noexcept
{
    AUTIL_LOG(INFO, "~LogicalFS [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
}

FSResult<void> LogicalFileSystem::Init(const FileSystemOptions& fileSystemOptions) noexcept
{
    AUTIL_LOG(INFO, "Begin init LogicalFS [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
    _options = std::make_shared<FileSystemOptions>(fileSystemOptions);
    if (_options->useRootLink) {
        _rootLinkPath = FILE_SYSTEM_ROOT_LINK_NAME;
        if (_options->rootLinkWithTs) {
            _rootLinkPath += "@" + StringUtil::toString(TimeUtility::currentTimeInSeconds());
        }
    }

    ScopedLock lock(*_lock);
    RETURN_IF_FS_ERROR(DoInit(), "");
    AUTIL_LOG(INFO, "End init LogicalFS [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
    return FSEC_OK;
}

// If versionId<0, Mount will not take effect and all existing data will be erased.
FSResult<void> LogicalFileSystem::ReInit(int versionId) noexcept
{
    AUTIL_LOG(INFO, "Begin re-init LogicalFS [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
    ScopedLock lock(*_lock);
    _inputStorage.reset();
    _outputStorage.reset();
    _entryTable->Clear();
    RETURN_IF_FS_ERROR(DoInit(), "");
    RETURN_IF_FS_ERROR(MountVersion(_outputRoot, versionId, "", FSMT_READ_ONLY, nullptr), "");
    AUTIL_LOG(INFO, "End re-init LogicalFS [%s] [%p] in [%s].", _name.c_str(), this, _outputRoot.c_str());
    return FSEC_OK;
}

// Reset FS to a version if versionId>=0. If versionId<0, reset FS to a complete new state.
FSResult<void> LogicalFileSystem::TEST_Reset(int versionId) noexcept { return ReInit(versionId); }

FSResult<void> LogicalFileSystem::DoInit() noexcept
{
    // ScopedLock lock(*_lock);
    // may be the threadLogicalFs, will inherit the entryTable of main thread
    if (!_entryTable) {
        _entryTable = _entryTableBuilder->CreateEntryTable(_name, _outputRoot, _options);
        assert(_outputRoot == _entryTable->GetOutputRoot());
    }

    RETURN_IF_FS_ERROR(InitStorage(), "");
    _metricsReporter->DeclareMetrics(GetFileSystemMetrics(), _options->metricPref);

    if (_options->isOffline) {
        auto ec = FslibWrapper::MkDir(_outputRoot, /*recursive=*/true, /*mayExist=*/true).Code();
        RETURN_IF_FS_ERROR(ec, "make root directory failed");
    }
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::CommitSelectedFilesAndDir(
    versionid_t versionId, const std::vector<std::string>& fileList, const std::vector<std::string>& dirList,
    const std::vector<std::string>& filterDirList, const std::string& finalDumpFileName,
    const std::string& finalDumpFileContent, FenceContext* fenceContext) noexcept
{
    AUTIL_LOG(INFO, "Begin commit version [%d] LogicalFS [%s] [%p] in [%s]", versionId, _name.c_str(), this,
              _outputRoot.c_str());
    assert(finalDumpFileName.empty() || PathUtil::GetFileName(finalDumpFileName) == finalDumpFileName);

    RETURN_IF_FS_ERROR(Sync(true).Code(), "commit version [%d] LogicalFS [%s] in [%s] falied", versionId, _name.c_str(),
                       _outputRoot.c_str());

    // TODO: optimize. to complete the entry meta tree
    for (const std::string& dir : dirList) {
        FSResult<EntryMeta> entryMetaRet = GetEntryMeta(dir);
        if (entryMetaRet.ec == FSEC_NOENT) {
            continue;
        }
        RETURN_IF_FS_ERROR(entryMetaRet.ec, "CommitSelectedFilesAndDir on dir [%s] failed", dir.c_str());
        const EntryMeta& entryMeta = entryMetaRet.result;
        if (!entryMeta.IsDir()) {
            AUTIL_LOG(ERROR, "[%s] is not a dir!", dir.c_str());
            return FSEC_NOTDIR;
        }
        if (entryMeta.IsLazy()) {
            auto ec = _entryTableBuilder->MountDirRecursive(entryMeta.GetPhysicalRoot(), dir, dir,
                                                            entryMeta.IsOwner() ? FSMT_READ_WRITE : FSMT_READ_ONLY);
            RETURN_IF_FS_ERROR(ec, "mount dir failed. root[%s] path[%s] to [%s]", entryMeta.GetPhysicalRoot().c_str(),
                               dir.c_str(), dir.c_str());
        }
    }
    for (const std::string& file : fileList) {
        FSResult<EntryMeta> entryMetaRet = GetEntryMeta(file);
        if (entryMetaRet.ec == FSEC_NOENT) {
            continue;
        }
        RETURN_IF_FS_ERROR(entryMetaRet.ec, "CommitSelectedFilesAndDir on file [%s] failed", file.c_str());
        const EntryMeta& entryMeta = entryMetaRet.result;
        if (entryMeta.IsDir()) {
            AUTIL_LOG(ERROR, "[%s] is a dir!", file.c_str());
            return FSEC_ISDIR;
        }
    }
    ScopedLock lock(*_lock);
    if (!finalDumpFileName.empty()) {
        auto [ec, entryMeta] = _entryTable->CreateFile(finalDumpFileName);
        RETURN_IF_FS_ERROR(ec, "create final file [%s] failed", finalDumpFileName.c_str());
        entryMeta.SetLength(finalDumpFileContent.size());
        ec = _entryTable->AddEntryMeta(entryMeta).ec;
        RETURN_IF_FS_ERROR(ec, "entry table add final dump file failed, [%s]", entryMeta.DebugString().c_str());
    }
    if (!_entryTable->Commit(versionId, fileList, dirList, filterDirList, fenceContext)) {
        if (!finalDumpFileName.empty()) {
            _entryTable->Delete(finalDumpFileName);
        }
        AUTIL_LOG(ERROR, "LogicalFS [%s] [%p] dump entry table failed", _name.c_str(), this);
        return FSEC_ERROR;
    }
    if (!finalDumpFileName.empty()) {
        string path = PathUtil::JoinPath(_outputRoot, finalDumpFileName);
        auto ec = FslibWrapper::AtomicStore(path, finalDumpFileContent, true, fenceContext).Code();
        if (ec != FSEC_OK) {
            _entryTable->Delete(finalDumpFileName);
            RETURN_IF_FS_ERROR(ec, "atomic store final dump file [%s] failed", path.c_str());
        }
    }

    // Update files that are commited above from mutable to immutable etc. See Update for more
    // details. This is necessary in order to:
    // 1. Maintain in memory EntryTable state to be consistent with physical files/dirs, so that
    // user can continue to e.g. read and write in current FS.
    // 2. Files that are committed e.g. should not be mutable, so we need to update this property.
    _entryTable->UpdateAfterCommit(fileList, dirList);

    AUTIL_LOG(INFO, "End commit LogicalFS [%s] [%p] in [%s]", _name.c_str(), this, _outputRoot.c_str());
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::CommitPreloadDependence(FenceContext* fenceContext) noexcept
{
    ScopedLock lock(*_lock);
    auto ret = EntryTableBuilder::GetLastVersion(_outputRoot);
    if (ret.ec != FSEC_OK && ret.ec != FSEC_NOENT) {
        AUTIL_LOG(ERROR, "get last version failed!");
        return FSEC_ERROR;
    }
    auto versionId = ret.result;
    if (versionId != -1) {
        AUTIL_LOG(
            ERROR,
            "LogicalFS [%s] [%p], other version file already exist! PreloadEntryTable must be the first to commit!",
            _name.c_str(), this);
        return FSEC_ERROR;
    }
    if (!_entryTable->CommitPreloadDependence(fenceContext)) {
        AUTIL_LOG(ERROR, "LogicalFS [%s] [%p] dump preload entry table failed", _name.c_str(), this);
        return FSEC_ERROR;
    }
    AUTIL_LOG(INFO, "End commit preloaddependence of LogicalFS [%s] [%p] in [%s]", _name.c_str(), this,
              _outputRoot.c_str());
    return FSEC_OK;
}

FSResult<void>
LogicalFileSystem::MountVersion(const std::string& rawPhysicalRoot, versionid_t versionId,
                                const std::string& rawLogicalPath, MountOption mountOption,
                                const std::shared_ptr<indexlib::file_system::LifecycleTable>& lifecycleTable) noexcept
{
    AUTIL_LOG(DEBUG,
              "MountVersion, physicalRoot[%s], versionId[%d], logicalPath[%s], mountType[%d], conflictResolution[%d]",
              rawPhysicalRoot.c_str(), versionId, rawLogicalPath.c_str(), mountOption.mountType,
              (int)mountOption.conflictResolution);

    if (unlikely(mountOption.mountType == FSMT_READ_WRITE)) {
        assert(false);
        AUTIL_LOG(ERROR, "only support mount version READ_ONLY till now, failed mount [%s] version [%d] to [%s]",
                  rawPhysicalRoot.c_str(), versionId, rawLogicalPath.c_str());
        return FSEC_NOTSUP;
    }

    string logicalPath = NormalizeLogicalPath(rawLogicalPath);
    string physicalRoot = PathUtil::NormalizePath(rawPhysicalRoot);
    if (versionId == INVALID_VERSIONID) {
        auto ret = EntryTableBuilder::GetLastVersion(physicalRoot);
        if (!ret.OK() && ret.ec != FSEC_NOENT) {
            AUTIL_LOG(ERROR, "list last version failed, root[%s], ec[%d]", physicalRoot.c_str(), ret.ec);
            return FSEC_ERROR;
        }
        versionId = ret.result;
    }

    ScopedLock lock(*_lock);
    if (lifecycleTable != nullptr) {
        AUTIL_LOG(INFO, "mount version[%d] with lifecycleTable[size=%zu]", versionId, lifecycleTable->Size());
        for (auto it = lifecycleTable->Begin(); it != lifecycleTable->End(); it++) {
            SetDirLifecycle(it->first, it->second);
        }
    }
    auto ec = _entryTableBuilder->MountVersion(physicalRoot, versionId, logicalPath, mountOption);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "mount version failed, physicalRoot [%s], vesion [%d], logicalPath [%s], ec[%d]",
                  physicalRoot.c_str(), versionId, logicalPath.c_str(), ec);
    }
    return ec;
}

FSResult<void> LogicalFileSystem::MountDir(const std::string& rawPhysicalRoot, const std::string& rawPhysicalPath,
                                           const std::string& rawLogicalPath, MountOption mountOption,
                                           bool enableLazyMount) noexcept
{
    AUTIL_LOG(DEBUG, "MountDir, physicalRoot[%s], physicalPath[%s], logicalPath[%s], mountType[%d]",
              rawPhysicalRoot.c_str(), rawPhysicalPath.c_str(), rawLogicalPath.c_str(), mountOption.mountType);
    std::string physicalRoot = PathUtil::NormalizePath(rawPhysicalRoot);
    std::string physicalPath = PathUtil::NormalizePath(rawPhysicalPath);
    std::string logicalPath = NormalizeLogicalPath(rawLogicalPath);
    auto mountType = mountOption.mountType;
    if (PathUtil::JoinPath(physicalRoot, physicalPath) == _outputRoot &&
        logicalPath.empty()) { // TODO:(qingran) tmp skip mount to root
        assert(mountType != FSMT_READ_ONLY);
        // if enableLazyMount is true, it's no need to do anything else since the outputRoot path is lazy loaded.
        if (enableLazyMount) {
            return FSEC_OK;
        }
    }

    if (mountType == FSMT_READ_ONLY || !enableLazyMount) {
        auto ec = _entryTableBuilder->MountDirRecursive(physicalRoot, physicalPath, logicalPath, mountOption);
        RETURN_IF_FS_ERROR(ec, "mount dir root[%s]/[%s] to [%s] failed", rawPhysicalRoot.c_str(),
                           rawPhysicalPath.c_str(), rawLogicalPath.c_str());
        return FSEC_OK;
    }

    if (mountOption.conflictResolution != ConflictResolution::OVERWRITE) {
        // TODO: support lazy mount
        AUTIL_LOG(ERROR, "lazy mount only suport ConflictResolution::OVERWRITE");
        return FSEC_BADARGS;
    }
    auto ret = FslibWrapper::IsDir(PathUtil::JoinPath(physicalRoot, physicalPath));
    RETURN_IF_FS_ERROR(ret.ec, "check dir exist failed when mount dir [%s]/[%s] to [%s]", physicalRoot.c_str(),
                       physicalPath.c_str(), logicalPath.c_str());
    if (!ret.result) {
        AUTIL_LOG(ERROR, "mount dir [%s]/[%s] to [%s] failed, physical path is not exist or not dir",
                  physicalRoot.c_str(), physicalPath.c_str(), logicalPath.c_str());
        return FSEC_NOTDIR;
    }
    auto ec = _entryTableBuilder->MountDirLazy(physicalRoot, physicalPath, logicalPath, mountOption);
    RETURN_IF_FS_ERROR(ec, "mount dir [%s]/[%s] to [%s] failed", physicalRoot.c_str(), physicalPath.c_str(),
                       logicalPath.c_str());
    AUTIL_LOG(INFO, "mount root [%s]/[%s] to [%s] type[%d] successfully", rawPhysicalRoot.c_str(),
              rawPhysicalPath.c_str(), rawLogicalPath.c_str(), mountType);
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::MountFile(const string& rawPhysicalRoot, const string& rawPhysicalPath,
                                            const string& rawLogicalPath, FSMountType mountType, int64_t length,
                                            bool mayNonExist) noexcept
{
    AUTIL_LOG(
        DEBUG,
        "MountFile, physicalRoot[%s], physicalPath[%s], logicalPath[%s], mountType[%d], length[%ld], mayNonExist[%d]",
        rawPhysicalRoot.c_str(), rawPhysicalPath.c_str(), rawLogicalPath.c_str(), mountType, length, mayNonExist);

    auto physicalRoot = PathUtil::NormalizePath(rawPhysicalRoot);
    auto physicalPath = PathUtil::NormalizePath(rawPhysicalPath);
    auto logicalPath = NormalizeLogicalPath(rawLogicalPath);

    ScopedLock lock(*_lock);

    auto ec = _entryTableBuilder->MountFile(physicalRoot, physicalPath, logicalPath, length, mountType);
    if (ec == FSEC_NOENT) {
        if (mayNonExist) {
            return FSEC_OK;
        }
        AUTIL_LOG(ERROR, "mount file failed. root[%s] path[%s] to [%s], not exist", rawPhysicalRoot.c_str(),
                  rawPhysicalPath.c_str(), rawLogicalPath.c_str());
    } else if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "mount file failed. root[%s] path[%s] to [%s], ec[%d]", rawPhysicalRoot.c_str(),
                  rawPhysicalPath.c_str(), rawLogicalPath.c_str(), ec);
    }
    return ec;
}

FSResult<void> LogicalFileSystem::MountSegment(const std::string& rawLogicalPath) noexcept
{
    auto path = NormalizeLogicalPath(rawLogicalPath);
    AUTIL_LOG(INFO, "Mount segment [%s]", rawLogicalPath.c_str());
    ScopedLock lock(*_lock);
    FSResult<EntryMeta> entryMetaRet = GetEntryMeta(path);
    RETURN_IF_FS_ERROR(entryMetaRet.ec, "Mount segment failed [%s]", rawLogicalPath.c_str());
    const EntryMeta& entryMeta = entryMetaRet.result;
    if (!entryMeta.IsDir()) {
        AUTIL_LOG(ERROR, "Mount segment failed, path [%s] is not dir", rawLogicalPath.c_str());
        return FSEC_ERROR;
    }

    if (!entryMeta.IsLazy()) {
        AUTIL_LOG(DEBUG, "Skip mount segment [%s] for it is not lazy", rawLogicalPath.c_str());
        return FSEC_OK;
    }
    auto ec = _entryTableBuilder->MountSegment(entryMeta.GetPhysicalRoot(), entryMeta.GetPhysicalPath(),
                                               entryMeta.GetLogicalPath(),
                                               entryMeta.IsOwner() ? FSMT_READ_WRITE : FSMT_READ_ONLY);
    RETURN_IF_FS_ERROR(ec, "Mount segment [%s] failed", rawLogicalPath.c_str());
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::CopyToOutputRoot(const std::string& logicalPath, bool mayNonExist) noexcept
{
    auto ret = GetPhysicalPath(logicalPath);
    if (!ret.OK()) {
        if (ret.ec == FSEC_NOENT && mayNonExist) {
            AUTIL_LOG(INFO, "Logical Path [%s] not exist!", logicalPath.c_str());
            return FSEC_OK;
        }
        RETURN_IF_FS_ERROR(ret.ec, "GetPhysicalPath [%s] failed", logicalPath.c_str());
    }
    const std::string& physicalPath = ret.result;

    std::string destPath = PathUtil::JoinPath(_outputRoot, logicalPath);
    if (physicalPath == destPath) {
        AUTIL_LOG(INFO, "Path [%s] already exist in OuputRoot [%s]", logicalPath.c_str(), _outputRoot.c_str());
        return FSEC_OK;
    }
    auto ec = FslibWrapper::Copy(physicalPath, destPath).Code();
    RETURN_IF_FS_ERROR(ec, "Path [%s] copy to OuputRoot failed [%s]", logicalPath.c_str(), _outputRoot.c_str());
    auto [ec2, isDir] = IsDir(logicalPath);
    RETURN_IF_FS_ERROR(ec2, "");
    if (isDir) {
        RETURN_IF_FS_ERROR(MountDir(_outputRoot, logicalPath, logicalPath, FSMT_READ_WRITE, true), "");
    } else {
        RETURN_IF_FS_ERROR(MountFile(_outputRoot, logicalPath, logicalPath, FSMT_READ_WRITE, -1, mayNonExist), "");
    }
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::MergeDirs(const std::vector<std::string>& physicalDirs,
                                            const std::string& logicalPath,
                                            const MergeDirsOption& mergeDirsOption) noexcept
{
    AUTIL_LOG(INFO, "merge dirs [%s] to [%s]", StringUtil::toString(physicalDirs).c_str(), logicalPath.c_str());
    if (physicalDirs.empty()) {
        return FSEC_OK;
    }
    auto [ec, dirEntryMeta] = _entryTable->GetEntryMetaMayLazy(logicalPath);
    std::string destPhysicalRoot =
        ec == FSEC_OK && !dirEntryMeta.IsMutable() ? _entryTable->GetPatchRoot() : _outputRoot;
    std::string destPhyicalFullPath = PathUtil::JoinPath(destPhysicalRoot, logicalPath);
    bool optimize = false;
    if (physicalDirs.size() == 1) {
        auto [ec, exist] = IsExist(logicalPath);
        RETURN_IF_FS_ERROR(ec, "merge dirs failed, isexist [%s] failed", logicalPath.c_str());
        optimize = !exist;
    }
    if (optimize) {
        // optimize for end build
        auto ec = FslibWrapper::Rename(physicalDirs[0], destPhyicalFullPath /*noFence*/).Code();
        if (ec == FSEC_NOENT) {
            AUTIL_LOG(WARN, "skip merge src dir [%s] for not exist", physicalDirs[0].c_str());
        } else {
            RETURN_IF_FS_ERROR(ec, "merge dirs failed, rename [%s] to [%s/%s]", physicalDirs[0].c_str(),
                               _outputRoot.c_str(), logicalPath.c_str());
        }
    } else {
        // normal for end Merge
        for (const std::string& physicalPath : physicalDirs) {
            auto isDirRet = FslibWrapper::IsDir(physicalPath);
            assert(isDirRet.Code() != FSEC_NOENT);
            if (isDirRet.OK() && isDirRet.result) {
                fslib::EntryList entryList;
                auto ec = FslibWrapper::ListDir(physicalPath, entryList).Code();
                RETURN_IF_FS_ERROR(ec, "Merge dirs failed, list src dir [%s] failed", physicalPath.c_str());
                for (size_t i = 0; i < entryList.size(); ++i) {
                    entryList[i].entryName = PathUtil::JoinPath(physicalPath, entryList[i].entryName);
                }
                ec = DirectoryMerger::MoveFiles(entryList, destPhyicalFullPath);
                RETURN_IF_FS_ERROR(ec, "merge dirs failed, move src dir [%s] to [%s]", physicalPath.c_str(),
                                   destPhyicalFullPath.c_str());
                continue;
            }
            RETURN_IF_FS_ERROR(isDirRet.Code(), "Merge dirs failed, check is dir [%s] failed", physicalPath.c_str());

            auto isFileRet = FslibWrapper::IsFile(physicalPath);
            assert(isFileRet.Code() != FSEC_NOENT);
            if (isFileRet.OK() && isFileRet.result) {
                auto ret = FslibWrapper::IsExist(physicalPath);
                RETURN_IF_FS_ERROR(ret.ec, "merge dirs failed, is exist [%s]", physicalPath.c_str());
                if (ret.result) {
                    auto ec = DirectoryMerger::MoveFiles({{false, physicalPath}}, destPhyicalFullPath);
                    RETURN_IF_FS_ERROR(ec, "merge dirs failed, move files [%s] to [%s]", physicalDirs[0].c_str(),
                                       destPhyicalFullPath.c_str());
                }
                continue;
            }
            RETURN_IF_FS_ERROR(isFileRet.Code(), "Merge dirs failed, check is dir [%s] failed", physicalPath.c_str());
        }
    }
    // mount merged dir and files
    auto ret = FslibWrapper::IsDir(PathUtil::JoinPath(destPhysicalRoot, logicalPath));
    RETURN_IF_FS_ERROR(ret.ec, "Merge dirs failed, isdir[%s/%s]", destPhysicalRoot.c_str(), logicalPath.c_str());
    if (ret.result) {
        RETURN_IF_FS_ERROR(MountDir(destPhysicalRoot, logicalPath, logicalPath, FSMT_READ_WRITE, true), "");
    } else {
        RETURN_IF_FS_ERROR(MountFile(destPhysicalRoot, logicalPath, logicalPath, FSMT_READ_WRITE, -1, false), "");
    }
    if (mergeDirsOption.mergePackageFiles) {
        RETURN_IF_FS_ERROR(MergePackageFiles(logicalPath, mergeDirsOption.fenceContext), "");
    }
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::MergePackageFiles(const std::string& logicalPath, FenceContext* fenceContext) noexcept
{
    FSResult<EntryMeta> dirEntryMetaRet = _entryTable->GetEntryMeta(logicalPath);
    const EntryMeta& dirEntryMeta = dirEntryMetaRet.result;
    std::string destPhysicalRoot =
        dirEntryMetaRet.ec == FSEC_OK && !dirEntryMeta.IsMutable() ? _entryTable->GetPatchRoot() : _outputRoot;
    std::string destPhyicalFullPath = PathUtil::JoinPath(destPhysicalRoot, logicalPath);

    FSResult<bool> mergePackageFilesRet = DirectoryMerger::MergePackageFiles(destPhyicalFullPath, fenceContext);
    RETURN_IF_FS_ERROR(mergePackageFilesRet.ec, "merge package files in [%s] failed", destPhyicalFullPath.c_str());

    auto [ec, exist] = IsExist(logicalPath);
    RETURN_IF_FS_ERROR(ec, "merge package files in [%s] failed, isexist [%s] failed", destPhyicalFullPath.c_str(),
                       logicalPath.c_str());
    if (exist) {
        // remove useless moved package files in entryTable.
        auto entryMetas = _entryTable->ListDir(logicalPath, true);
        for (const auto& entryMeta : entryMetas) {
            _entryTable->Delete(entryMeta);
        }
        // re mount
        RETURN_IF_FS_ERROR(MountDir(destPhysicalRoot, logicalPath, logicalPath, FSMT_READ_WRITE, true), "");
    }
    if (mergePackageFilesRet.result) {
        RETURN_IF_FS_ERROR(MakeDirectory(logicalPath, DirectoryOption()), "MakeDirectory [%s]", logicalPath.c_str());
        auto ec = _entryTableBuilder->MountPackageDir(destPhysicalRoot, logicalPath, logicalPath, FSMT_READ_WRITE);
        if (ec != FSEC_NOENT) {
            RETURN_IF_FS_ERROR(ec, "merge package file in [%s] failed", destPhyicalFullPath.c_str());
        }
    }
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::InitStorage() noexcept
{
    AUTIL_LOG(INFO, "init storage, outputRoot [%s], options [%s]", _outputRoot.c_str(),
              _options->DebugString().c_str());

    if (_options->memoryQuotaControllerV2) {
        _memController.reset(new BlockMemoryQuotaController(_options->memoryQuotaControllerV2, "file_system"));
    } else {
        assert(_options->memoryQuotaController);
        _memController.reset(new BlockMemoryQuotaController(_options->memoryQuotaController, "file_system"));
    }

    _inputStorage = Storage::CreateInputStorage(_options, _memController, _entryTable, _lock.get());
    if (!_inputStorage) {
        AUTIL_LOG(ERROR, "Create input storage failed! output root [%s]", _outputRoot.c_str());
        return FSEC_ERROR;
    }
    _outputStorage =
        Storage::CreateOutputStorage(_outputRoot, _options, _memController, _name, _entryTable, _lock.get());
    if (!_outputStorage) {
        AUTIL_LOG(ERROR, "Create output stoage failed, output root [%s]", _outputRoot.c_str());
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<EntryMeta> LogicalFileSystem::CreateEntryMeta(const string& path) noexcept
{
    ScopedLock lock(*_lock);
    auto [ec, isDir] = IsDir(PathUtil::GetParentDirPath(path));
    RETURN2_IF_FS_ERROR(ec, EntryMeta(), "Create file writer failed, IsDir parent of path [%s] failed", path.c_str());
    if (!isDir) {
        RETURN2_IF_FS_ERROR(FSEC_ERROR, EntryMeta(),
                            "Create file writer failed, Parent dir for file [%s] not exist or is not dir",
                            path.c_str());
    }
    auto ret = _entryTable->CreateFile(path);
    if (ret.Code() != FSEC_OK) {
        AUTIL_LOG(ERROR, "Create file writer failed, file [%s], ec[%d]", path.c_str(), ret.Code());
        return ret;
    }
    assert(ret.result.IsMutable());
    return ret;
}

FSResult<std::shared_ptr<FileWriter>> LogicalFileSystem::CreateFileWriter(const string& rawPath,
                                                                          const WriterOption& rawWriterOption) noexcept
{
    AUTIL_LOG(DEBUG, "file[%s], openType[%d]", rawPath.c_str(), rawWriterOption.openType);

    string path = NormalizeLogicalPath(rawPath);
    WriterOption writerOption = rawWriterOption;
    writerOption.metricGroup = GetMetricGroup(path);
    if (writerOption.openType == FSOT_SLICE) {
        return CreateSliceFileWriter(path, writerOption);
    } else if (writerOption.openType == FSOT_MEM) {
        return CreateMemFileWriter(path, writerOption);
    }

    ScopedLock lock(*_lock);

    bool deleteWhenFail = false;
    string fullPhysicalPath;
    auto [ec, entryMeta] = GetEntryMeta(path);
    if (ec == FSEC_OK) {
        fullPhysicalPath = entryMeta.GetFullPhysicalPath();
        if (writerOption.openType == FSOT_MEM) {
            AUTIL_LOG(INFO, "file [%s] already exist, remove it [%s] before CreateFileWriter!", path.c_str(),
                      fullPhysicalPath.c_str());
            RETURN2_IF_FS_ERROR(RemoveFile(path, RemoveOption()), std::shared_ptr<FileWriter>(), "remove [%s] failed",
                                path.c_str());
        } else if (!writerOption.isAppendMode) {
            AUTIL_LOG(ERROR, "Create file [%s] => [%s] writer failed, already exist", rawPath.c_str(),
                      fullPhysicalPath.c_str());
            return {FSEC_EXIST, std::shared_ptr<FileWriter>()};
        }
    } else if (ec == FSEC_NOENT) {
        auto [ec1, entryMeta1] = CreateEntryMeta(path);
        RETURN2_IF_FS_ERROR(ec1, std::shared_ptr<FileWriter>(), "CreateEntryMeta failed, path[%s]", path.c_str());
        if (writerOption.noDump) { // noDump means pure memory file
            entryMeta1.SetIsMemFile(true);
        }
        auto addRet = _entryTable->AddEntryMeta(entryMeta1);
        RETURN2_IF_FS_ERROR(addRet.ec, std::shared_ptr<FileWriter>(), "entry table add file failed, [%s]",
                            entryMeta1.DebugString().c_str());
        fullPhysicalPath = entryMeta1.GetFullPhysicalPath();
        deleteWhenFail = true;
    } else {
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "Create file [%s] writer failed", rawPath.c_str());
    }

    auto ret = _outputStorage->CreateFileWriter(path, fullPhysicalPath, writerOption);
    if (ret.ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "Create file [%s] => [%s] writer failed", rawPath.c_str(), fullPhysicalPath.c_str());
        if (deleteWhenFail) {
            _entryTable->Delete(path);
        }
    }
    return ret;
}

FSMetricGroup LogicalFileSystem::GetMetricGroup(const std::string& normPath) noexcept
{
    for (const std::string& rootPath : _options->memMetricGroupPaths) {
        if (PathUtil::IsInRootPath(normPath, rootPath)) {
            return FSMG_MEM;
        }
    }
    return FSMG_LOCAL;
}

FSResult<std::shared_ptr<FileReader>> LogicalFileSystem::CreateFileReader(const string& rawPath,
                                                                          const ReaderOption& readerOption) noexcept
{
    bool isLink = false;
    auto path = NormalizeLogicalPath(rawPath, &isLink);

    AUTIL_LOG(DEBUG, "file[%s], openType[%d], isLink[%d]", rawPath.c_str(), readerOption.openType, isLink);

    if (readerOption.openType == FSOT_SLICE) {
        return CreateSliceFileReader(path, readerOption);
    }

    std::shared_ptr<FileReader> reader;
    {
        ScopedLock lock(*_lock);
        auto [ec, entryMeta] = GetEntryMeta(path);
        if (ec != FSEC_OK) {
            if (ec == FSEC_NOENT && readerOption.mayNonExist) {
                AUTIL_LOG(DEBUG, "get entry meta failed [%s]", rawPath.c_str());
            } else {
                AUTIL_LOG(ERROR, "get entry meta failed [%s]", rawPath.c_str());
            }
            return {ec, std::shared_ptr<FileReader>()};
        }
        ReaderOption newReaderOption(readerOption);
        newReaderOption.fileLength = entryMeta.GetLength();
        newReaderOption.fileOffset = entryMeta.GetOffset();
        newReaderOption.linkRoot = isLink ? _rootLinkPath : "";

        auto storage = GetStorage(entryMeta, isLink);

        std::string physicalPath = entryMeta.GetFullPhysicalPath();
        if (readerOption.forceRemotePath) {
            physicalPath = entryMeta.GetRawFullPhysicalPath();
        }

        // logical path should contain linkroot, for inside/outside cache & CompressFileAddressMapper check
        auto [ec2, fileReader] = storage->CreateFileReader(/*logicalFilePath*/ PathUtil::NormalizePath(rawPath),
                                                           physicalPath, newReaderOption);
        RETURN2_IF_FS_ERROR(ec2, std::shared_ptr<FileReader>(), "Open file [%s] for reading failed, address [%s]",
                            path.c_str(), entryMeta.GetFullPhysicalPath().c_str());
        reader = fileReader;
    }
    assert(reader);
    // avoid lock, Open need populate data will take much time
    RETURN2_IF_FS_ERROR(reader->Open(), std::shared_ptr<FileReader>(), "reader open failed, file[%s]", rawPath.c_str());
    reader->InitMetricReporter(GetFileSystemMetricsReporter());
    return {FSEC_OK, reader};
}

FSResult<ResourceFilePtr> LogicalFileSystem::CreateResourceFile(const string& rawPath) noexcept
{
    string path = NormalizeLogicalPath(rawPath);
    AUTIL_LOG(DEBUG, "File[%s]", rawPath.c_str());
    ScopedLock lock(*_lock);
    auto [ec, file] = GetResourceFile(path);
    if (ec == FSEC_OK && file != nullptr) {
        return {FSEC_OK, file};
    }
    RETURN2_IF_FS_ERROR(ec, ResourceFilePtr(), "GetResourceFile [%s] failed", rawPath.c_str());
    auto [ec2, entryMeta] = CreateEntryMeta(path);
    RETURN2_IF_FS_ERROR(ec2, ResourceFilePtr(), "CreateEntryMeta failed, path[%s]", path.c_str());
    entryMeta.SetIsMemFile(true);
    bool addToEntryTable = !_options->isOffline;
    auto ret = _outputStorage->CreateResourceFile(path, entryMeta.GetFullPhysicalPath(), GetMetricGroup(path));
    RETURN2_IF_FS_ERROR(ret.ec, ResourceFilePtr(), "Create ResourceFile [%s] => [%s] failed", rawPath.c_str(),
                        entryMeta.GetFullPhysicalPath().c_str());
    if (addToEntryTable) {
        auto ec = _entryTable->AddEntryMeta(entryMeta).Code();
        RETURN2_IF_FS_ERROR(ec, ResourceFilePtr(), "Entry table add InMemoryFile failed, [%s]",
                            entryMeta.DebugString().c_str());
    }

    if (ret.result) {
        // Set Clean callback
        ret.result->SetFileNodeCleanCallback([this, path] {
            ScopedLock lock(*_lock);
            _entryTable->Delete(path);
        });
        ret.result->UpdateFileLengthMetric(GetFileSystemMetricsReporter());
    }
    return {FSEC_OK, ret.result};
}

FSResult<ResourceFilePtr> LogicalFileSystem::GetResourceFile(const string& rawPath) const noexcept
{
    string path = NormalizeLogicalPath(rawPath);
    AUTIL_LOG(DEBUG, "File[%s]", rawPath.c_str());
    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = _entryTable->GetEntryMeta(path);
    if (ec != FSEC_OK) {
        return {FSEC_OK, ResourceFilePtr()}; // for compitable
    }
    if (!entryMeta.IsMutable()) {
        AUTIL_LOG(ERROR, "Unexpected mutable state, path:[%s].", rawPath.c_str());
        return {FSEC_ERROR, ResourceFilePtr()};
    }
    auto [ec2, resourceFile] = _outputStorage->GetResourceFile(path);
    RETURN2_IF_FS_ERROR(ec2, ResourceFilePtr(), "get ResourceFile [%s] failed", rawPath.c_str());
    return {FSEC_OK, resourceFile};
}

FSResult<void> LogicalFileSystem::MakeDirectory(const std::string& rawPath,
                                                const DirectoryOption& directoryOption) noexcept
{
    string path = NormalizeLogicalPath(rawPath);
    AUTIL_LOG(INFO, "Make dir [%s], recursive[%d]", rawPath.c_str(), directoryOption.recursive);

    if (unlikely(path.empty())) {
        return FSEC_OK;
    }

    ScopedLock lock(*_lock);

    auto [ec, entryMeta] = GetEntryMeta(path);
    if (ec == FSEC_OK) {
        if (!directoryOption.recursive) {
            AUTIL_LOG(ERROR, "Make dir [%s] failed, already exist", rawPath.c_str());
            return FSEC_EXIST;
        }
        if (!entryMeta.IsDir()) {
            AUTIL_LOG(ERROR, "Make dir [%s] failed, is a file", rawPath.c_str());
            return FSEC_EXIST;
        }
        ec = _outputStorage->MakeDirectory(path, entryMeta.GetFullPhysicalPath(), directoryOption.recursive,
                                           directoryOption.packageHint);
        RETURN_IF_FS_ERROR(ec, "Make dir [%s] failed, physical path [%s], recursive [%d]", path.c_str(),
                           entryMeta.GetFullPhysicalPath().c_str(), directoryOption.recursive);
        return FSEC_OK;
    } else if (ec != FSEC_NOENT) {
        RETURN_IF_FS_ERROR(ec, "Make dir [%s] failed", rawPath.c_str());
    }

    std::vector<EntryMeta> metas;
    // Allocate EntryMeta
    ec = _entryTable->MakeDirectory(path, directoryOption.recursive, &metas);
    if (ec == FSEC_EXIST) {
        AUTIL_LOG(ERROR, "make dir [%s] failed, already exist", rawPath.c_str());
        return FSEC_EXIST;
    } else if (ec == FSEC_NOENT) {
        AUTIL_LOG(ERROR, "make dir [%s] failed, parent dir not exist", rawPath.c_str());
        return FSEC_NOENT;
    } else if (unlikely(ec != FSEC_OK)) {
        AUTIL_LOG(ERROR, "make dir [%s] failed, unexpected error [%d]", rawPath.c_str(), ec);
        return FSEC_ERROR;
    }

    if (metas.size() > 0) {
        // Make directory in physical storage
        ec = _outputStorage->MakeDirectory(path, metas.back().GetFullPhysicalPath(), directoryOption.recursive,
                                           directoryOption.packageHint);
        RETURN_IF_FS_ERROR(ec, "Make dir [%s] failed, physical path [%s], recursive [%d]", path.c_str(),
                           metas.back().GetFullPhysicalPath().c_str(), directoryOption.recursive);
    }
    for (auto& meta : metas) {
        ec = _entryTable->AddEntryMeta(meta).ec;
        RETURN_IF_FS_ERROR(ec, "Make dir [%s] failed, entryMeta[%s]", path.c_str(), meta.DebugString().c_str());
    }
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::RemoveFile(const string& rawPath, const RemoveOption& removeOption) noexcept
{
    AUTIL_LOG(INFO, "Remove File[%s] in [%s] mayNonExist[%d]", rawPath.c_str(), _outputRoot.c_str(),
              removeOption.mayNonExist);

    bool isLink = false;
    string path = NormalizeLogicalPath(rawPath, &isLink);

    if (unlikely(path.empty())) {
        AUTIL_LOG(ERROR, "Remove root dir is forbidden");
        return FSEC_BADARGS;
    }

    // make sure to remove the file in flushing
    RETURN_IF_FS_ERROR(Sync(true).Code(), "sync [%s] true failed", rawPath.c_str());

    std::unique_lock<autil::RecursiveThreadMutex> lock(*_lock);
    if (removeOption.relaxedConsistency) {
        lock.unlock();
    }

    auto [ec, entryMeta] = GetEntryMeta(path);
    if (ec == FSEC_NOENT) {
        if (removeOption.mayNonExist) {
            return FSEC_OK;
        }
        RETURN_IF_FS_ERROR(ec, "Get EntryMeta failed, path [%s]", rawPath.c_str());
    }
    if (entryMeta.IsDir()) {
        AUTIL_LOG(ERROR, "Remove file [%s] failed, is directory!", rawPath.c_str());
        return FSEC_ISDIR;
    }
    string fullPhysicalPath = entryMeta.GetFullPhysicalPath();
    if (!entryMeta.IsOwner()) {
        AUTIL_LOG(DEBUG, "Remove file [%s] not owned.", path.c_str());
    } else if (isLink) {
        auto ec = _inputStorage->RemoveFile(path, fullPhysicalPath, removeOption.fenceContext);
        if (unlikely(ec != FSEC_OK && ec != FSEC_NOENT)) {
            RETURN_IF_FS_ERROR(ec, "Remove file [%s] => [%s] failed", rawPath.c_str(), fullPhysicalPath.c_str());
        }
        ec = _outputStorage->RemoveFile(path, fullPhysicalPath, removeOption.fenceContext);
        if (unlikely(ec != FSEC_OK && ec != FSEC_NOENT)) {
            RETURN_IF_FS_ERROR(ec, "Remove file [%s] => [%s] failed", rawPath.c_str(), fullPhysicalPath.c_str());
        }
    } else {
        auto storage = GetStorage(entryMeta, isLink);
        // meta for slice file will be released after storage call RemoveFile and becomes invalid.
        auto ec = storage->RemoveFile(path, fullPhysicalPath, removeOption.fenceContext);
        if (ec != FSEC_OK && ec != FSEC_NOENT) {
            RETURN_IF_FS_ERROR(ec, "Remove file [%s] => [%s] failed", rawPath.c_str(), fullPhysicalPath.c_str());
        }
    }
    _entryTable->Delete(path);
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::RemoveDirectory(const std::string& rawPath, const RemoveOption& removeOption) noexcept
{
    AUTIL_LOG(INFO, "Remove directory [%s] in [%s], mayNonExist[%d]", rawPath.c_str(), _outputRoot.c_str(),
              removeOption.mayNonExist);

    string path = NormalizeLogicalPath(rawPath);

    if (unlikely(path.empty())) {
        AUTIL_LOG(ERROR, "Remove root dir is forbidden");
        return FSEC_BADARGS;
    }

    // make sure to remove the file in flushing
    RETURN_IF_FS_ERROR(Sync(true).Code(), "sync [%s] true failed", rawPath.c_str());

    std::unique_lock<autil::RecursiveThreadMutex> lock(*_lock);
    auto [ec, removeRootMeta] = GetEntryMeta(path);
    if (ec == FSEC_NOENT && removeOption.mayNonExist) {
        return FSEC_OK;
    }
    RETURN_IF_FS_ERROR(ec, "Get EntryMeta failed, path [%s]", rawPath.c_str());

    if (!removeRootMeta.IsDir()) {
        AUTIL_LOG(ERROR, "Remove directory [%s] failed. not a directory", rawPath.c_str());
        return FSEC_NOTDIR;
    }

    auto entryMetas = _entryTable->ListDir(path, true);
    entryMetas.emplace(entryMetas.begin(), removeRootMeta);
    string removeRootPhysicalPath = removeRootMeta.GetPhysicalPath();

    std::unordered_set<string> removeRoots;
    for (const auto& entryMeta : entryMetas) {
        if (entryMeta.IsOwner() && !entryMeta.IsInPackage()) {
            removeRoots.insert(entryMeta.GetPhysicalRoot());
        } else {
            // inherit from other builder or merger
            AUTIL_LOG(DEBUG, "Remove directory [%s] which is not owner", entryMeta.GetFullPhysicalPath().c_str());
        }
        // TODO: maybe we can optimize
        _entryTable->Delete(entryMeta);
    }
    ec = _entryTable->OptimizeMemoryStructures();
    RETURN_IF_FS_ERROR(ec, "Remove directory [%s] failed", path.c_str());
    if (removeOption.relaxedConsistency) {
        lock.unlock();
    }

    if (removeOption.logicalDelete) {
        AUTIL_LOG(INFO, "remove directory [%s] logical, not remove physical path", rawPath.c_str());
        return FSEC_OK;
    }

    for (const string& removeRoot : removeRoots) {
        auto removePath = PathUtil::JoinPath(removeRoot, removeRootPhysicalPath);
        auto ec = _outputStorage->RemoveDirectory(path, removePath, removeOption.fenceContext);
        if (ec == FSEC_OK || (ec == FSEC_NOENT && removeOption.mayNonExist)) {
            continue;
        }
        RETURN_IF_FS_ERROR(ec, "Remove directory [%s] => [%s] failed", path.c_str(), removePath.c_str());
    }
    return FSEC_OK;
}

FSResult<bool> LogicalFileSystem::IsExist(const string& rawPath) const noexcept
{
    bool isLink = false;
    string path = NormalizeLogicalPath(rawPath, &isLink);
    if (path.empty()) {
        return {FSEC_OK, true};
    }
    if (isLink) {
        auto ret = FslibWrapper::IsExist(PathUtil::JoinPath(_outputRoot, path));
        if (ret.ec != FSEC_OK) {
            AUTIL_LOG(ERROR, "check file IsExist [%s] failed, ec [%d]", PathUtil::JoinPath(_outputRoot, path).c_str(),
                      ret.ec);
            return {ret.ec, false};
        }
        if (!ret.result) {
            return {FSEC_OK, false};
        }
    }

    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    if (ec == FSEC_OK) {
        return {FSEC_OK, true};
    } else if (ec != FSEC_NOENT) {
        AUTIL_LOG(ERROR, "get entry meta [%s] failed, ec [%d]", rawPath.c_str(), ec);
        return {FSEC_ERROR, false};
    }
    return {FSEC_OK, false};
}

FSResult<bool> LogicalFileSystem::IsDir(const string& rawPath) const noexcept
{
    string path = NormalizeLogicalPath(rawPath);
    if (path.empty()) {
        return {FSEC_OK, true};
    }
    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    if (ec == FSEC_OK) {
        return {FSEC_OK, entryMeta.IsDir()};
    } else if (ec == FSEC_NOENT) {
        return {FSEC_OK, false};
    }
    AUTIL_LOG(ERROR, "Get entry meta [%s] failed", rawPath.c_str());
    return {ec, false};
}

FSResult<void> LogicalFileSystem::ListDir(const std::string& rawPath, const ListOption& listOption,
                                          std::vector<std::string>& fileList) const noexcept
{
    AUTIL_LOG(DEBUG, "Dir[%s], recursive[%d]", rawPath.c_str(), listOption.recursive);
    string path = NormalizeLogicalPath(rawPath);
    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    RETURN_IF_FS_ERROR(ec, "List dir [%s] failed", rawPath.c_str());
    if (!entryMeta.IsDir()) {
        AUTIL_LOG(ERROR, "List dir [%s] failed, not a dir!", rawPath.c_str());
        return FSEC_NOTDIR;
    }

    if (entryMeta.IsLazy()) {
        if (!listOption.recursive) {
            auto ec = FslibWrapper::ListDir(entryMeta.GetFullPhysicalPath(), fileList).Code();
            RETURN_IF_FS_ERROR(ec, "List dir [%s] failed", entryMeta.GetFullPhysicalPath().c_str());
            FileList::iterator iter = std::remove_if(fileList.begin(), fileList.end(), FslibWrapper::IsTempFile);
            fileList.erase(iter, fileList.end());
            // we do not want to get length to complete the entry table
        } else {
            auto ec = _entryTableBuilder->MountDirRecursive(entryMeta.GetPhysicalRoot(), path, path,
                                                            entryMeta.IsOwner() ? FSMT_READ_WRITE : FSMT_READ_ONLY);
            if (ec != FSEC_NOENT) {
                RETURN_IF_FS_ERROR(ec, "Mount dir [%s] failed", entryMeta.GetFullPhysicalPath().c_str());
            }
        }
    }

    auto metas = _entryTable->ListDir(path, listOption.recursive);
    for (const auto& meta : metas) {
        auto logicalPath = meta.GetLogicalPath();
        auto pos = logicalPath.rfind('.');
        if (pos == string::npos || logicalPath.substr(pos) != TEMP_FILE_SUFFIX) {
            if (!path.empty()) {
                logicalPath = logicalPath.substr(path.size() + 1);
            }

            if (listOption.recursive && meta.IsDir()) {
                logicalPath = PathUtil::NormalizeDir(logicalPath);
            }

            fileList.emplace_back(logicalPath);
        }
    }
    if (entryMeta.IsLazy() && !listOption.recursive) {
        std::sort(fileList.begin(), fileList.end());
        auto iter = std::unique(fileList.begin(), fileList.end());
        fileList.erase(iter, fileList.end());
    }
    return FSEC_OK;
}

FSResult<void> LogicalFileSystem::ListDir(const std::string& rawPath, const ListOption& listOption,
                                          std::vector<FileInfo>& fileInfos) const noexcept
{
    AUTIL_LOG(DEBUG, "Dir[%s], recursive[%d]", rawPath.c_str(), listOption.recursive);
    string path = NormalizeLogicalPath(rawPath);
    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    RETURN_IF_FS_ERROR(ec, "List dir [%s] failed", rawPath.c_str());
    if (!entryMeta.IsDir()) {
        AUTIL_LOG(ERROR, "List dir [%s] failed, not a dir!", rawPath.c_str());
        return FSEC_NOTDIR;
    }

    if (entryMeta.IsLazy()) {
        auto ec = _entryTableBuilder->MountDirRecursive(entryMeta.GetPhysicalRoot(), path, path,
                                                        entryMeta.IsOwner() ? FSMT_READ_WRITE : FSMT_READ_ONLY);
        if (ec != FSEC_NOENT) {
            RETURN_IF_FS_ERROR(ec, "mount dir failed. root[%s] path[%s] to [%s]", entryMeta.GetPhysicalRoot().c_str(),
                               path.c_str(), path.c_str());
        }
    }

    auto metas = _entryTable->ListDir(path, listOption.recursive);
    fileInfos.reserve(metas.size());
    for (const auto& meta : metas) {
        auto logicalPath = meta.GetLogicalPath();
        auto pos = logicalPath.rfind('.');
        if (pos == string::npos || logicalPath.substr(pos) != TEMP_FILE_SUFFIX) {
            FileInfo info;
            if (!path.empty()) {
                logicalPath = logicalPath.substr(path.size() + 1);
            }

            if (listOption.recursive && meta.IsDir()) {
                logicalPath = PathUtil::NormalizeDir(logicalPath);
            }

            info.filePath = logicalPath;
            if (meta.IsFile()) {
                info.fileLength = meta.GetLength();
            }
            fileInfos.emplace_back(info);
        }
    }
    return FSEC_OK;
}

// ListPhysicalFile should get all physical files of rawPath from entry-table directly, instead of
// using e.g. entry-meta to get package data then find corresponding package meta name.
FSResult<void> LogicalFileSystem::ListPhysicalFile(const std::string& rawPath, const ListOption& listOption,
                                                   std::vector<FileInfo>& fileInfos) noexcept
{
    AUTIL_LOG(DEBUG, "Dir[%s], recursive[%d]", rawPath.c_str(), listOption.recursive);
    string path = NormalizeLogicalPath(rawPath);

    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    RETURN_IF_FS_ERROR(ec, "List dir [%s] failed", rawPath.c_str());
    if (!entryMeta.IsDir()) {
        AUTIL_LOG(ERROR, "List dir [%s] failed, not a dir!", rawPath.c_str());
        return FSEC_NOTDIR;
    }
    ec = _entryTableBuilder->MountDirRecursive(entryMeta.GetPhysicalRoot(), path, path,
                                               entryMeta.IsOwner() ? FSMT_READ_WRITE : FSMT_READ_ONLY);
    if (ec != FSEC_NOENT) {
        RETURN_IF_FS_ERROR(ec, "mount dir failed. root[%s] path[%s] to [%s]", entryMeta.GetPhysicalRoot().c_str(),
                           path.c_str(), path.c_str());
    }
    std::unordered_set<std::string> filter;
    auto metas = _entryTable->ListDir(path, listOption.recursive);
    fileInfos.reserve(metas.size());
    for (const auto& meta : metas) {
        if (meta.IsMemFile()) {
            continue;
        }
        auto physicalPath = meta.GetPhysicalPath();
        auto pos = physicalPath.rfind('.');
        if (pos == string::npos || physicalPath.substr(pos) != TEMP_FILE_SUFFIX) {
            if (!path.empty()) {
                physicalPath = physicalPath.substr(path.size() + 1);
            }

            if (meta.IsInPackage() && filter.find(physicalPath) == filter.end()) {
                filter.insert(physicalPath);
                auto [ec, packDataFileLen] = _entryTable->GetPackageFileLength(meta.GetRawFullPhysicalPath());
                RETURN_IF_FS_ERROR(ec, "GetPackageFileLength [%s] failed", meta.GetRawFullPhysicalPath().c_str());
                fileInfos.emplace_back(physicalPath, packDataFileLen);
                // dir is skipped because dir's physical path points to package meta file path.
                if (meta.IsDir()) {
                    continue;
                }

                auto packMetaPath = GetPackageMetaFilePath(physicalPath);
                if (filter.find(packMetaPath) == filter.end()) {
                    filter.insert(packMetaPath);
                    std::string packMetaFilePath =
                        PathUtil::JoinPath(meta.GetRawPhysicalRoot(), GetPackageMetaFilePath(meta.GetPhysicalPath()));
                    auto [ec, packMetaFileLen] = _entryTable->GetPackageFileLength(packMetaFilePath);
                    RETURN_IF_FS_ERROR(ec, "GetPackageFileLength [%s] failed", packMetaFilePath.c_str());
                    fileInfos.emplace_back(packMetaPath, packMetaFileLen);
                }
            } else if (!meta.IsInPackage()) {
                if (listOption.recursive && meta.IsDir()) {
                    physicalPath = PathUtil::NormalizeDir(physicalPath);
                }
                fileInfos.emplace_back(physicalPath, meta.IsDir() ? -1 : meta.GetLength());
            }
        }
    }
    return FSEC_OK;
}

FSResult<size_t> LogicalFileSystem::GetFileLength(const std::string& rawPath) const noexcept
{
    string path = NormalizeLogicalPath(rawPath);

    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    RETURN2_IF_FS_ERROR(ec, 0, "Get file [%s] length failed, get entry meta failed", rawPath.c_str());
    if (entryMeta.IsDir()) {
        return {FSEC_OK, 0};
    }
    assert(entryMeta.GetLength() >= 0);
    return {FSEC_OK, (size_t)entryMeta.GetLength()};
}

FSResult<void> LogicalFileSystem::TEST_GetFileStat(const std::string& rawPath, FileStat& fileStat) const noexcept
{
    bool isLink = false;
    string path = NormalizeLogicalPath(rawPath, &isLink);

    ScopedLock lock(*_lock);
    FSResult<EntryMeta> entryMetaRet = GetEntryMeta(path);
    RETURN_IF_FS_ERROR(entryMetaRet.ec, "get file [%s] entry meta failed", rawPath.c_str());
    const EntryMeta& entryMeta = entryMetaRet.result;

    if (unlikely(entryMeta.IsDir())) {
        assert(false);
        fileStat.isDirectory = true;
    } else {
        fileStat.isDirectory = false;
        auto [ec, fileLength] = GetFileLength(path);
        RETURN_IF_FS_ERROR(ec, "GetFileLength [%s] failed", path.c_str());
        fileStat.fileLength = fileLength;
    }

    fileStat.inPackage = entryMeta.IsInPackage();
    fileStat.offset = entryMeta.GetOffset();
    fileStat.physicalRoot = entryMeta.GetPhysicalRoot();
    fileStat.physicalPath = entryMeta.GetPhysicalPath();
    auto storage = GetStorage(entryMeta, isLink);
    storage->TEST_GetFileStat(PathUtil::NormalizePath(rawPath), fileStat);
    return FSEC_OK;
}

FSResult<std::shared_future<bool>> LogicalFileSystem::Sync(bool waitFinish) noexcept
{
    ScopedLock lock(_syncLock);
    if (_future.valid()) {
        _future.wait();
    }

    std::future<bool> retFuture;
    {
        // TODO(xingwo) fix dead lock
        ScopedLock innerLock(*_lock);
        auto [ec, future] = _outputStorage->Sync();
        RETURN2_IF_FS_ERROR(ec, std::move(future), "Sync failed");
        retFuture = std::move(future);
    }

    if (waitFinish) {
        // Attention: future.wait() not equal to MemStorage.WaitDumpQueueEmpty(), the latter will rethrow exceptions
        // occures in async flush thread, but future.wait() will not
        auto ec = _outputStorage->WaitSyncFinish();
        if (ec != FSEC_OK) {
            RETURN2_IF_FS_ERROR(ec, retFuture.share(), "WaitSyncFinish failed");
        }
    }

    _future = retFuture.share();
    return {FSEC_OK, _future};
}

FSResult<void> LogicalFileSystem::FlushPackage(const string& rawLogicalDirPath) noexcept
{
    if (_options->outputStorage != FSST_PACKAGE_MEM) {
        return FSEC_OK;
    }

    bool isLink = false;
    auto logicalDirPath = NormalizeLogicalPath(rawLogicalDirPath, &isLink);
    assert(!isLink);

    ScopedLock lock(*_lock);
    return _outputStorage->FlushPackage(logicalDirPath);
}

FSResult<int32_t> LogicalFileSystem::CommitPackage() noexcept
{
    assert(_options->outputStorage == FSST_PACKAGE_DISK);

    ScopedLock lock(*_lock);
    return _outputStorage->CommitPackage();
}

FSResult<void> LogicalFileSystem::RecoverPackage(int32_t checkpoint, const std::string& rawLogicalDirPath,
                                                 const std::vector<std::string>& physicalFileListHint) noexcept
{
    assert(_options->outputStorage == FSST_PACKAGE_DISK);

    std::string logicalDirPath = NormalizeLogicalPath(rawLogicalDirPath);
    AUTIL_LOG(INFO, "Begin recover dir [%s] with [%d] in [%s]", logicalDirPath.c_str(), checkpoint,
              _outputRoot.c_str());
    assert(GetEntryMeta(logicalDirPath).result.GetPhysicalPath() ==
           GetEntryMeta(logicalDirPath).result.GetLogicalPath());
    auto ec = _outputStorage->RecoverPackage(checkpoint, logicalDirPath, physicalFileListHint);
    RETURN_IF_FS_ERROR(ec, "Recover [%s] with checkpoint [%d] failed, [%lu] files", logicalDirPath.c_str(), checkpoint,
                       physicalFileListHint.size());
    AUTIL_LOG(INFO, "End recover dir [%s] with [%d] in [%s]", logicalDirPath.c_str(), checkpoint, _outputRoot.c_str());
    return FSEC_OK;
}

FileSystemMetrics LogicalFileSystem::GetFileSystemMetrics() const noexcept
{
    ScopedLock lock(*_lock);
    const StorageMetrics& inputStorageMetrics = _inputStorage->GetMetrics();
    const StorageMetrics& outputStorageMetrics = _outputStorage->GetMetrics();
    return FileSystemMetrics(inputStorageMetrics, outputStorageMetrics);
}

void LogicalFileSystem::ReportMetrics() noexcept
{
    ScopedLock lock(*_lock);
    _metricsReporter->ReportMetrics(GetFileSystemMetrics());
    if (_memController) {
        _metricsReporter->ReportMemoryQuotaUse(_memController->GetUsedQuota());
    }
}

bool LogicalFileSystem::IsExistInCache(const std::string& rawPath) const noexcept
{
    bool isLink = false;
    string path = NormalizeLogicalPath(rawPath, &isLink);
    ScopedLock lock(*_lock);
    FSResult<EntryMeta> entryMetaRet = _entryTable->GetEntryMeta(path);
    if (entryMetaRet.ec != FSEC_OK) {
        return false;
    }
    auto storage = GetStorage(entryMetaRet.result, isLink);
    return storage->IsExistInCache(PathUtil::NormalizePath(rawPath));
}

FSResult<FSFileType> LogicalFileSystem::DeduceFileType(const std::string& rawPath, FSOpenType openType) noexcept
{
    AUTIL_LOG(DEBUG, "File[%s], OpenType[%d]", rawPath.c_str(), openType);

    bool isLink = false;
    string path = NormalizeLogicalPath(rawPath, &isLink);

    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    RETURN2_IF_FS_ERROR(ec, FSFT_UNKNOWN, "get file [%s] entry meta failed", rawPath.c_str());
    if (!entryMeta.IsFile()) {
        AUTIL_LOG(ERROR, "Path [%s] is not regular file!", rawPath.c_str());
        return {FSEC_ERROR, FSFT_UNKNOWN};
    }
    auto storage = GetStorage(entryMeta, isLink);
    return storage->DeduceFileType(PathUtil::NormalizePath(rawPath), entryMeta.GetFullPhysicalPath(), openType);
}

FSResult<size_t> LogicalFileSystem::EstimateFileLockMemoryUse(const string& rawPath, FSOpenType openType) noexcept
{
    AUTIL_LOG(DEBUG, "File[%s], OpenType[%d]", rawPath.c_str(), openType);

    bool isLink = false;
    string path = NormalizeLogicalPath(rawPath, &isLink);

    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    RETURN2_IF_FS_ERROR(ec, 0ul, "get file [%s] entry meta failed", rawPath.c_str());
    if (!entryMeta.IsFile()) {
        AUTIL_LOG(ERROR, "Path [%s] is not regular file!", rawPath.c_str());
        return {FSEC_ERROR, 0ul};
    }

    auto storage = GetStorage(entryMeta, isLink);
    // logical path should contain linkroot, for inside/outside cache & CompressFileAddressMapper check
    return storage->EstimateFileLockMemoryUse(PathUtil::NormalizePath(rawPath), entryMeta.GetFullPhysicalPath(),
                                              openType, entryMeta.GetLength());
}

FSResult<size_t> LogicalFileSystem::EstimateFileMemoryUseChange(const string& rawPath,
                                                                const std::string& oldTemperature,
                                                                const std::string& newTemperature) noexcept
{
    AUTIL_LOG(DEBUG, "File[%s], oldTemperature[%s], newTemperature[%s]", rawPath.c_str(), oldTemperature.c_str(),
              newTemperature.c_str());

    bool isLink = false;
    string path = NormalizeLogicalPath(rawPath, &isLink);

    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    RETURN2_IF_FS_ERROR(ec, 0ul, "get file [%s] entry meta failed", rawPath.c_str());
    if (!entryMeta.IsFile()) {
        AUTIL_LOG(ERROR, "Path [%s] is not regular file!", rawPath.c_str());
        return {FSEC_ERROR, 0ul};
    }

    auto storage = GetStorage(entryMeta, isLink);
    // logical path should contain linkroot, for inside/outside cache & CompressFileAddressMapper check
    return storage->EstimateFileMemoryUseChange(PathUtil::NormalizePath(rawPath), entryMeta.GetFullPhysicalPath(),
                                                oldTemperature, newTemperature, entryMeta.GetLength());
}

void LogicalFileSystem::SwitchLoadSpeedLimit(bool on) noexcept { _options->loadConfigList.SwitchLoadSpeedLimit(on); }

bool LogicalFileSystem::SetDirLifecycle(const string& rawPath, const string& lifecycle) noexcept
{
    auto path = NormalizeLogicalPath(rawPath);
    ScopedLock lock(*_lock);
    auto r1 = _inputStorage->SetDirLifecycle(path, lifecycle);
    auto r2 = _outputStorage->SetDirLifecycle(path, lifecycle);
    auto r3 = _entryTableBuilder->SetDirLifecycle(path, lifecycle);
    return r1 && r2 && (r3 == FSEC_OK);
}

FSResult<FSStorageType> LogicalFileSystem::GetStorageType(const std::string& rawPath) const noexcept
{
    bool isLink = false;
    auto path = NormalizeLogicalPath(rawPath, &isLink);

    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = GetEntryMeta(path);
    if (ec == FSEC_NOENT) {
        return {FSEC_OK, FSST_UNKNOWN};
    } else if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "get file [%s] entry meta failed", rawPath.c_str());
        return {ec, FSST_UNKNOWN};
    }
    auto storage = GetStorage(entryMeta, isLink);
    return {FSEC_OK, storage->GetStorageType()};
}

FSResult<void> LogicalFileSystem::Validate(const string& rawPath) const noexcept
{
    // TODO: support validate package file
    string path = NormalizeLogicalPath(rawPath);

    ScopedLock lock(*_lock);
    FSResult<EntryMeta> entryMetaRet = _entryTable->GetEntryMeta(path);
    if (entryMetaRet.ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "validate [%s] failed, not existed in entry table", rawPath.c_str());
        return FSEC_ERROR;
    }
    const EntryMeta& entryMeta = entryMetaRet.result;
    auto [ec, pathMeta] = FslibWrapper::GetPathMeta(entryMeta.GetFullPhysicalPath());
    if (ec == FSEC_NOENT) {
        AUTIL_LOG(ERROR, "validate [%s] failed, not exist on disk [%s]", rawPath.c_str(),
                  entryMeta.DebugString().c_str());
        return FSEC_NOENT;
    } else if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "validate [%s] failed, get path meta error [%d]", rawPath.c_str(), ec);
        return FSEC_ERROR;
    }

    if (entryMeta.IsDir() != (!pathMeta.isFile)) {
        AUTIL_LOG(ERROR, "validate [%s] failed, on disk isDir[%d], in entry table [%s]", rawPath.c_str(),
                  !pathMeta.isFile, entryMeta.DebugString().c_str());
        return FSEC_ERROR;
    }
    if (entryMeta.IsFile() && !entryMeta.IsInPackage() && entryMeta.GetLength() != pathMeta.length) {
        AUTIL_LOG(ERROR, "validate [%s] failed, on disk length[%ld], in entry table [%s]", rawPath.c_str(),
                  pathMeta.length, entryMeta.DebugString().c_str());
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

void LogicalFileSystem::CleanCache() noexcept
{
    ScopedLock lock(*_lock);
    _inputStorage->CleanCache();
    _outputStorage->CleanCache();
}

FSResult<void> LogicalFileSystem::Rename(const std::string& rawSrcPath, const std::string& rawDestPath,
                                         FenceContext* fenceContext) noexcept
{
    // TODO: add ut & support recover
    AUTIL_LOG(INFO, "rename [%s] => [%s]", rawSrcPath.c_str(), rawDestPath.c_str());
    auto srcPath = NormalizeLogicalPath(rawSrcPath);
    auto destPath = NormalizeLogicalPath(rawDestPath);

    ScopedLock lock(*_lock);

    FSResult<EntryMeta> ret = GetEntryMeta(srcPath);
    RETURN_IF_FS_ERROR(ret.ec, "get src file [%s] entry meta failed", rawSrcPath.c_str());
    auto ec = _entryTable->Rename(srcPath, destPath, fenceContext);
    RETURN_IF_FS_ERROR(ec, "Rename failed, [%s] => [%s]", rawSrcPath.c_str(), rawDestPath.c_str());
    return FSEC_OK;
}

void LogicalFileSystem::TEST_SetUseRootLink(bool useRootLink) noexcept
{
    _options->useRootLink = useRootLink;
    _rootLinkPath.clear();
    if (_options->useRootLink) {
        _rootLinkPath = FILE_SYSTEM_ROOT_LINK_NAME;
        if (_options->rootLinkWithTs) {
            _rootLinkPath += "@" + StringUtil::toString(TimeUtility::currentTimeInSeconds());
        }
    }
}

void LogicalFileSystem::TEST_MountLastVersion() noexcept(false)
{
    static const string versionFileNamePrefix = "version.";
    static const string entryTableFileNamePrefix = "entry_table.";
    FileList fileList;
    auto ec = FslibWrapper::ListDir(_outputRoot, fileList).Code();
    THROW_IF_FS_ERROR(ec, "ListDir in TEST_MountLastVersion failed: %s", _outputRoot.c_str());

    // TODO: rm
    int32_t lastVersionId = -1;
    for (const string& fileName : fileList) {
        versionid_t versionId = 0;
        if (autil::StringUtil::startsWith(fileName, versionFileNamePrefix) &&
            autil::StringUtil::fromString((string)(fileName.substr(versionFileNamePrefix.size())), versionId)) {
            lastVersionId = std::max(lastVersionId, versionId);
        }
    }
    if (lastVersionId < 0) {
        for (const string& fileName : fileList) {
            versionid_t versionId = 0;
            if (autil::StringUtil::startsWith(fileName, entryTableFileNamePrefix) &&
                autil::StringUtil::fromString((string)(fileName.substr(entryTableFileNamePrefix.size())), versionId)) {
                lastVersionId = std::max(lastVersionId, versionId);
            }
        }
    }
    THROW_IF_FS_ERROR(MountVersion(_outputRoot, lastVersionId, "", FSMT_READ_ONLY, nullptr), "mount version failed");
}

std::string LogicalFileSystem::NormalizeLogicalPath(const std::string& rawPath, bool* isLink) const noexcept
{
    auto path = PathUtil::NormalizePath(rawPath);
    // logical fs should not get absolute path
    assert(unlikely(path[0] != '/' && path.find("://") == string::npos));

    // TODO: need a more effective path format, such as link=ts@LOGICAL_PATH
    // Trim link path prefix
    if (!_rootLinkPath.empty() && path.find(_rootLinkPath) == 0) {
        if (path.size() == _rootLinkPath.size()) {
            path.clear();
        } else {
            path = path.substr(_rootLinkPath.size() + 1);
        }
        if (isLink) {
            *isLink = true;
        }
    }
    return path;
}

util::MemoryReserverPtr LogicalFileSystem::CreateMemoryReserver(const std::string& name) noexcept
{
    return util::MemoryReserverPtr(new util::MemoryReserver(name, _memController));
}

FSResult<string> LogicalFileSystem::GetPhysicalPath(const std::string& rawLogicalPath) const noexcept
{
    std::string logicalPath = NormalizeLogicalPath(rawLogicalPath);
    ScopedLock lock(*_lock);
    FSResult<EntryMeta> entryMetaRet = GetEntryMeta(logicalPath);
    if (!entryMetaRet.OK()) {
        assert(entryMetaRet.ec == FSEC_NOENT || entryMetaRet.ec == FSEC_ERROR);
        return {entryMetaRet.ec, ""};
    }
    const EntryMeta& entryMeta = entryMetaRet.result;
    return {FSEC_OK, entryMeta.GetFullPhysicalPath()};
}

FSResult<void> LogicalFileSystem::TEST_Recover(int32_t checkpoint, const std::string& rawLogicalDirPath) noexcept
{
    std::string logicalDirPath = NormalizeLogicalPath(rawLogicalDirPath);
    std::vector<std::string> physicalFileList;
    FSResult<EntryMeta> entryMetaRet = GetEntryMeta(logicalDirPath);
    if (entryMetaRet.OK()) {
        auto ec = FslibWrapper::ListDir(entryMetaRet.result.GetFullPhysicalPath(), physicalFileList).Code();
        RETURN_IF_FS_ERROR(ec, "List dir [%s] => [%s] failed", logicalDirPath.c_str(),
                           entryMetaRet.result.GetFullPhysicalPath().c_str());
    }
    return RecoverPackage(checkpoint, rawLogicalDirPath, physicalFileList);
}

FSResult<void> LogicalFileSystem::TEST_Commit(int versionId, const std::string& finalDumpFileName,
                                              const std::string& finalDumpFileContent) noexcept
{
    return CommitSelectedFilesAndDir(versionId, {}, {}, {}, finalDumpFileName, finalDumpFileContent,
                                     FenceContext::NoFence());
};

bool LogicalFileSystem::TEST_GetPhysicalInfo(const std::string& logicalPath, std::string& physicalRoot,
                                             std::string& relativePath, bool& inPackage, bool& isDir) const noexcept
{
    ScopedLock lock(*_lock);
    const std::string path = NormalizeLogicalPath(logicalPath);
    return _entryTable->TEST_GetPhysicalInfo(path, physicalRoot, relativePath, inPackage, isDir);
}

void LogicalFileSystem::SetDefaultRootPath(const std::string& defaultLocalPath,
                                           const std::string& defaultRemotePath) noexcept
{
    AUTIL_LOG(INFO, "fs root: local[%s], remote[%s]", defaultLocalPath.c_str(), defaultRemotePath.c_str());
    ScopedLock lock(*_lock);
    _options->loadConfigList.SetDefaultRootPath(PathUtil::NormalizePath(defaultLocalPath),
                                                PathUtil::NormalizePath(defaultRemotePath));
}

std::string LogicalFileSystem::DebugString() const noexcept { return _name + ":" + _outputRoot; }

std::string LogicalFileSystem::GetPackageMetaFilePath(const std::string& packageDataFileName) noexcept
{
    auto pos = packageDataFileName.find(PACKAGE_FILE_DATA_SUFFIX);
    assert(pos != std::string::npos);
    return packageDataFileName.substr(0, pos) + PACKAGE_FILE_META_SUFFIX;
}

Storage* LogicalFileSystem::GetStorage(const EntryMeta& entryMeta, bool isLink) const noexcept
{
    if (isLink) {
        return _inputStorage.get();
    }
    if (entryMeta.IsMutable()) {
        return _outputStorage.get();
    }
    // online will use output storage even though flushed. eg: rt segment deletionmap
    // see ut OnlinePartitionInteTest/OnlinePartitionInteTestMode.TestRtRemoveRtDoc/0 for more detail
    if (!_options->isOffline && _outputStorage->IsExistInCache(entryMeta.GetLogicalPath())) {
        return _outputStorage.get();
    }
    return _inputStorage.get();
}

FSResult<EntryMeta> LogicalFileSystem::GetEntryMeta(const std::string& logicalPath) const noexcept
{
    return _entryTable->GetEntryMetaMayLazy(logicalPath);
}

FSResult<std::shared_ptr<FileReader>>
LogicalFileSystem::CreateSliceFileReader(const string& path, const ReaderOption& readerOption) noexcept
{
    AUTIL_LOG(DEBUG, "CreateSliceFileReader[%s]", path.c_str());
    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = _entryTable->GetEntryMeta(path);
    if (ec == FSEC_NOENT) {
        return {FSEC_OK, std::shared_ptr<FileReader>()}; // for compitable
    }
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileReader>(), "GetEntryMeta [%s] failed", path.c_str());
    auto [ec2, sliceFileReader] =
        _outputStorage->CreateSliceFileReader(path, entryMeta.GetFullPhysicalPath(), readerOption);
    if (ec2 == FSEC_NOENT) {
        return {FSEC_OK, std::shared_ptr<FileReader>()}; // for compitable
    }
    RETURN2_IF_FS_ERROR(ec2, std::shared_ptr<FileReader>(), "CreateSliceFileReader[%s->%s] failed", path.c_str(),
                        entryMeta.GetFullPhysicalPath().c_str());
    if (sliceFileReader) {
        sliceFileReader->InitMetricReporter(GetFileSystemMetricsReporter());
    }
    return {FSEC_OK, sliceFileReader};
}

FSResult<std::shared_ptr<FileWriter>>
LogicalFileSystem::CreateSliceFileWriter(const string& path, const WriterOption& writerOption) noexcept
{
    AUTIL_LOG(DEBUG, "CreateSliceFileWriter[%s], sliceLen[%lu], sliceNum[%d]", path.c_str(), writerOption.sliceLen,
              writerOption.sliceNum);
    ScopedLock lock(*_lock);
    auto [ec, entryMeta] = CreateEntryMeta(path);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "CreateEntryMeta failed, path[%s]", path.c_str());
    // @qingran all not flushed on disk files include slice file should not display in entry_table.X
    // slice file does not need fs patch. it is a pure memory file without sync to disk
    entryMeta.SetIsMemFile(true);
    entryMeta.SetPhysicalRoot(_entryTable->GetPhysicalRootPointer(_entryTable->GetOutputRoot()));
    auto ret = _outputStorage->CreateSliceFileWriter(path, entryMeta.GetFullPhysicalPath(), writerOption);
    RETURN2_IF_FS_ERROR(ret.Code(), std::shared_ptr<FileWriter>(), "CreateSliceFileWriter[%s] => [%s] failed",
                        path.c_str(), entryMeta.GetFullPhysicalPath().c_str());
    ec = _entryTable->AddEntryMeta(entryMeta).ec;
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "Entry table add SliceFile failed, [%s]",
                        entryMeta.DebugString().c_str());
    return ret;
}

FSResult<std::shared_ptr<FileWriter>> LogicalFileSystem::CreateMemFileWriter(const string& path,
                                                                             const WriterOption& writerOption) noexcept
{
    ScopedLock lock(*_lock);

    if (IsExistInCache(path)) {
        AUTIL_LOG(INFO, "file [%s] already exist, remove it before CreateFileWriter!", path.c_str());
        RETURN2_IF_FS_ERROR(RemoveFile(path, RemoveOption()), std::shared_ptr<FileWriter>(), "remove file [%s] failed",
                            path.c_str());
    }

    auto [ec, entryMeta] = CreateEntryMeta(path);
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "CreateEntryMeta failed, path[%s]", path.c_str());
    auto ret = _outputStorage->CreateMemFileWriter(path, entryMeta.GetFullPhysicalPath(), writerOption);
    RETURN2_IF_FS_ERROR(ret.ec, std::shared_ptr<FileWriter>(), "Create file [%s] => [%s] writer failed", path.c_str(),
                        entryMeta.GetFullPhysicalPath().c_str());
    entryMeta.SetLength(writerOption.fileLength);
    entryMeta.SetIsMemFile(true);
    ec = _entryTable->AddEntryMeta(entryMeta).ec;
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "Entry table add InMemoryFile failed, [%s]",
                        entryMeta.DebugString().c_str());
    return ret;
}

FSResult<std::unique_ptr<IFileSystem>>
LogicalFileSystem::CreateThreadOwnFileSystem(const std::string& name) const noexcept
{
    std::unique_ptr<LogicalFileSystem> threadFileSystem =
        std::make_unique<LogicalFileSystem>(name, _outputRoot, nullptr);
    threadFileSystem->_lock = _lock;
    threadFileSystem->_entryTableBuilder = _entryTableBuilder;
    threadFileSystem->_entryTable = _entryTable;
    threadFileSystem->_metricsReporter = _metricsReporter;
    auto ec = threadFileSystem->Init(*_options);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "init threadFileSystem failed");
        return {ec, nullptr};
    }
    return {FSEC_OK, std::move(threadFileSystem)};
}

void LogicalFileSystem::TEST_EnableSupportMmap(bool isEnable)
{
    if (_inputStorage != nullptr) {
        _inputStorage->TEST_EnableSupportMmap(isEnable);
    }
    if (_outputStorage != nullptr) {
        _outputStorage->TEST_EnableSupportMmap(isEnable);
    }
}

FSResult<void> LogicalFileSystem::CalculateVersionFileSize(const std::string& rawPhysicalRoot,
                                                           const std::string& rawLogicalPath,
                                                           versionid_t versionId) noexcept
{
    if (versionId == INVALID_VERSIONID) {
        return FSEC_OK;
    }
    {
        ScopedLock lock(*_lock);
        if (_versionFileSizes.find(versionId) != _versionFileSizes.end()) {
            return FSEC_OK;
        }
    }
    const std::string logicalPath = NormalizeLogicalPath(rawLogicalPath);
    const std::string physicalRoot = PathUtil::NormalizePath(rawPhysicalRoot);

    auto entryTableBuilder = std::make_unique<EntryTableBuilder>();
    auto entryTable = entryTableBuilder->CreateEntryTable(_name, _outputRoot, _options);
    assert(_outputRoot == entryTable->GetOutputRoot());

    auto ec =
        entryTableBuilder->MountVersion(physicalRoot, versionId, logicalPath, indexlib::file_system::FSMT_READ_ONLY);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "mount version failed, physicalRoot [%s], vesion [%d], logicalPath [%s], ec[%d]",
                  physicalRoot.c_str(), versionId, logicalPath.c_str(), ec);
        return ec;
    }
    const size_t totalFileSize = entryTable->CalculateFileSize();

    ScopedLock lock(*_lock);
    _versionFileSizes[versionId] = totalFileSize;
    return FSEC_OK;
}

size_t LogicalFileSystem::GetVersionFileSize(versionid_t versionId) const noexcept
{
    size_t fileSize = 0;
    ScopedLock lock(*_lock);
    auto it = _versionFileSizes.find(versionId);
    if (it != _versionFileSizes.end()) {
        fileSize = it->second;
    }
    return fileSize;
}
}} // namespace indexlib::file_system

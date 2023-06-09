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
#include "indexlib/file_system/package/PackageDiskStorage.h"

#include <algorithm>
#include <assert.h>
#include <iterator>
#include <ostream>
#include <type_traits>
#include <unistd.h>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/BufferedPackageFileWriter.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PackageDiskStorage);

PackageDiskStorage::PackageDiskStorage(bool isReadonly, const string& rootPath, const string& fsName,
                                       const BlockMemoryQuotaControllerPtr& memController,
                                       std::shared_ptr<EntryTable> entryTable) noexcept
    : Storage(isReadonly, memController, entryTable)
    , _description(fsName)
    , _rootPath(rootPath)
    , _fileAlignSize(getpagesize())
    , _isDirty(false)
{
    assert(!isReadonly);
}

PackageDiskStorage::~PackageDiskStorage() noexcept
{
    for (Unit& unit : _units) {
        if (!unit.unflushedBuffers.empty()) {
            // AUTIL_LOG(ERROR, "*** PackageDiskStorage has unFlushedBuffer!");
            unit.unflushedBuffers.clear();
        }
    }
}

ErrorCode PackageDiskStorage::Init(const std::shared_ptr<FileSystemOptions>& options) noexcept
{
    _options = options;
    if (options->packageFileTagConfigList) {
        _tagFunction = ([options](const string& logicalPath) {
            return options->packageFileTagConfigList->Match(logicalPath, "");
        });
    }
    return FSEC_OK;
}

FSResult<std::shared_ptr<FileWriter>> PackageDiskStorage::CreateFileWriter(const std::string& logicalFilePath,
                                                                           const std::string& physicalFilePath,
                                                                           const WriterOption& writerOption) noexcept
{
    AUTIL_LOG(DEBUG, "Create file writer [%s] => [%s], length[%ld]", logicalFilePath.c_str(), physicalFilePath.c_str(),
              writerOption.fileLength);

    assert(writerOption.openType == FSOT_UNKNOWN || writerOption.openType == FSOT_BUFFERED);

    auto updateFileSizeFunc = [this, logicalFilePath](int64_t size) {
        _entryTable && _entryTable->UpdateFileSize(logicalFilePath, size);
    };

    if (writerOption.notInPackage) {
        assert(false); // for now do merge do't need this
        BufferedFileWriterPtr fileWriter(new BufferedFileWriter(writerOption, std::move(updateFileSizeFunc)));
        auto ec = fileWriter->Open(logicalFilePath, physicalFilePath);
        RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "FileWriter Open [%s] ==> [%s] failed",
                            logicalFilePath.c_str(), physicalFilePath.c_str());
        return {FSEC_OK, fileWriter};
    }
    int32_t unitId = -1;
    {
        ScopedLock lock(_lock);
        unitId = MatchUnit(logicalFilePath);
    }
    if (unlikely(unitId < 0)) {
        AUTIL_LOG(ERROR, "file [%s] not find match any unit!", logicalFilePath.c_str());
        return {FSEC_NOENT, std::shared_ptr<FileWriter>()};
    }
    std::shared_ptr<FileWriter> fileWriter(
        new BufferedPackageFileWriter(this, unitId, writerOption, std::move(updateFileSizeFunc)));
    auto ec = fileWriter->Open(logicalFilePath, "");
    RETURN2_IF_FS_ERROR(ec, std::shared_ptr<FileWriter>(), "FileWriter Open [%s] ==> [%s] failed",
                        logicalFilePath.c_str(), physicalFilePath.c_str());
    return {FSEC_OK, fileWriter};
}

ErrorCode PackageDiskStorage::StoreFile(const std::shared_ptr<FileNode>& fileNode,
                                        const WriterOption& writerOption) noexcept
{
    if (FSFT_MEM != fileNode->GetType()) {
        AUTIL_LOG(ERROR, "Package disk storage not support store [%d] file [%s]", fileNode->GetType(),
                  fileNode->DebugString().c_str());
        return FSEC_NOTSUP;
    }
    const string& logicalFilePath = fileNode->GetLogicalPath();
    WriterOption newWriterOption(writerOption);
    newWriterOption.openType = FSOT_UNKNOWN; // newWriterOption.openType may be FSOT_MEM
    auto [ec, writer] = CreateFileWriter(logicalFilePath, fileNode->GetPhysicalPath(), newWriterOption);
    RETURN_IF_FS_ERROR(ec, "create file wirter [%s] failed", logicalFilePath.c_str());
    RETURN_IF_FS_ERROR(writer->Write(fileNode->GetBaseAddress(), fileNode->GetLength()).Code(),
                       "write file [%s] failed", fileNode->DebugString().c_str());
    RETURN_IF_FS_ERROR(writer->Close(), "close file [%s] failed", fileNode->DebugString().c_str());
    ec = _entryTable->Freeze(logicalFilePath);
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "freeze file [%s] failed, ec [%d]", fileNode->DebugString().c_str(), ec);
    }
    return ec;
}

ErrorCode PackageDiskStorage::MakeDirectory(const std::string& logicalDirPath, const std::string& physicalDirPath,
                                            bool recursive, bool packageHint) noexcept
{
    ScopedLock lock(_lock);
    int32_t unitId = MatchUnit(logicalDirPath);
    if (packageHint) {
        if (unitId >= 0) {
            if (logicalDirPath.size() > _units[unitId].rootPath.size()) {
                AUTIL_LOG(INFO, "New root path [%s] match existing root path [%s]", logicalDirPath.c_str(),
                          _units[unitId].rootPath.c_str());
            } else {
                AUTIL_LOG(INFO, "Root path [%s] match with existing root path [%s]", logicalDirPath.c_str(),
                          _units[unitId].rootPath.c_str());
                return recursive ? FSEC_OK : FSEC_EXIST;
            }
        }
        Unit unit(logicalDirPath);
        _units.push_back(std::move(unit));
        return FSEC_OK;
    }

    if (unlikely(unitId < 0)) {
        assert(false);
        AUTIL_LOG(ERROR, "Directory [%s] not find match root!", logicalDirPath.c_str());
        return FSEC_NOTSUP;
    }
    auto& unit = _units[unitId];
    if (recursive) {
        return MakeDirectoryRecursive(unit, logicalDirPath);
    }
    if (unit.metas.find(logicalDirPath) != unit.metas.end()) {
        AUTIL_LOG(ERROR, "dir [%s] already exist", logicalDirPath.c_str());
        return FSEC_EXIST;
    }
    return DoMakeDirectory(unit, logicalDirPath);
}

ErrorCode PackageDiskStorage::MakeDirectoryRecursive(Unit& unit, const string& logicalDirPath) noexcept
{
    if (logicalDirPath == unit.rootPath) {
        return FSEC_OK;
    }
    string parentPath = PathUtil::GetParentDirPath(logicalDirPath);
    auto iter = unit.metas.find(parentPath);
    if (iter == unit.metas.end()) {
        ErrorCode ec = MakeDirectoryRecursive(unit, parentPath);
        if (ec != FSEC_OK) {
            return ec;
        }
    } else if (!iter->second.IsDir()) {
        AUTIL_LOG(ERROR, "path[%s] already exist and is not dir", iter->second.GetFilePath().c_str());
        return FSEC_NOTDIR;
    }
    assert(IsExist(parentPath));
    return DoMakeDirectory(unit, logicalDirPath);
}

ErrorCode PackageDiskStorage::DoMakeDirectory(Unit& unit, const string& logicalDirPath) noexcept
{
    string path = logicalDirPath.substr(unit.rootPath.size() + 1);
    if (unit.metas.count(path) == 0) {
        InnerFileMeta meta(path, true);
        unit.metas.insert(make_pair(logicalDirPath, meta));
    }
    return FSEC_OK;
}

bool PackageDiskStorage::IsExist(const string& logicalPath) const noexcept
{
    ScopedLock lock(_lock);
    int32_t unitId = MatchUnit(logicalPath);
    if (unitId < 0) {
        return false;
    }
    if (logicalPath == _units[unitId].rootPath) {
        return true;
    }
    return _units[unitId].metas.find(logicalPath) != _units[unitId].metas.end();
}

ErrorCode PackageDiskStorage::FlushPackage(const std::string& logicalDirPath) noexcept
{
    assert(false);
    return FSEC_NOTSUP;
}

ErrorCode PackageDiskStorage::FlushBuffers() noexcept
{
    ScopedLock lock(_lock);
    if (!_isDirty) {
        return FSEC_OK;
    }
    for (uint32_t unitId = 0; unitId < _units.size(); ++unitId) {
        Unit& unit = _units[unitId];
        for (auto& unflushedFile : unit.unflushedBuffers) {
            auto [ec, physicalFileId, stream] = DoAllocatePhysicalStream(unit, unflushedFile.logicalFilePath);
            RETURN_IF_FS_ERROR(ec, "DoAllocatePhysicalStream failed");
            FileBuffer* buffer = unflushedFile.buffer.get();
            RETURN_IF_FS_ERROR(stream->Write(buffer->GetBaseAddr(), buffer->GetCursor()).Code(), "write failed");
            RETURN_IF_FS_ERROR(
                StoreLogicalFile(unitId, unflushedFile.logicalFilePath, physicalFileId, buffer->GetCursor()),
                "StoreLogicalFile failed");
        }
        unit.unflushedBuffers.clear();
    }
    _isDirty = false;
    return FSEC_OK;
}

FSResult<std::shared_ptr<FileReader>> PackageDiskStorage::CreateFileReader(const std::string& logicalFilePath,
                                                                           const std::string& physicalFilePath,
                                                                           const ReaderOption& readerOption) noexcept
{
    AUTIL_LOG(ERROR, "not supported read");
    return {FSEC_NOTSUP, std::shared_ptr<FileReader>()};
}

ErrorCode PackageDiskStorage::RemoveFile(const string& logicalFilePath, const string& physicalFilePath,
                                         FenceContext*) noexcept
{
    assert(!IsExist(logicalFilePath));
    assert(false);
    return FSEC_NOTSUP;
}

ErrorCode PackageDiskStorage::RemoveDirectory(const string& logicalDirPath, const string& physicalDirPath,
                                              FenceContext*) noexcept
{
    assert(!IsExist(logicalDirPath));
    assert(false);
    return FSEC_NOTSUP;
}

FSResult<FSFileType> PackageDiskStorage::DeduceFileType(const std::string& logicalPath, const std::string& physicalPath,
                                                        FSOpenType type) noexcept
{
    assert(false);
    AUTIL_LOG(ERROR, "PackageDiskStorage not support DeduceFileType, [%s => %s]", logicalPath.c_str(),
              physicalPath.c_str());
    return {FSEC_NOTSUP, FSFT_UNKNOWN};
}

FSResult<size_t> PackageDiskStorage::EstimateFileLockMemoryUse(const string& logicalPath, const string& physicalPath,
                                                               FSOpenType type, int64_t fileLength) noexcept
{
    assert(false);
    AUTIL_LOG(ERROR, "PackageDiskStorage not support EstimateFileLockMemoryUse, [%s => %s]", logicalPath.c_str(),
              physicalPath.c_str());
    return {FSEC_NOTSUP, 0u};
}

FSResult<size_t> PackageDiskStorage::EstimateFileMemoryUseChange(const std::string& logicalPath,
                                                                 const std::string& physicalPath,
                                                                 const std::string& oldTemperature,
                                                                 const std::string& newTemperature,
                                                                 int64_t fileLength) noexcept
{
    assert(false);
    AUTIL_LOG(ERROR, "PackageDiskStorage not support EstimateFileMemoryUseChange, [%s => %s]", logicalPath.c_str(),
              physicalPath.c_str());
    return {FSEC_NOTSUP, 0u};
}

ErrorCode PackageDiskStorage::RecoverPackage(int32_t checkpoint, const std::string& logicalDir,
                                             const std::vector<std::string>& physicalFileListHint) noexcept
{
    AUTIL_LOG(INFO, "recover package storage [%s/%s], metaid[%d] in [%s]", _rootPath.c_str(), logicalDir.c_str(),
              checkpoint, _description.c_str());
    FSResult<EntryMeta> metaRet = _entryTable->GetEntryMetaMayLazy(logicalDir);
    if (metaRet.ec != FSEC_OK) {
        return FSEC_NOENT;
    }
    ScopedLock lock(_lock);
    int32_t unitId = MatchUnit(logicalDir);
    if (unlikely(unitId) < 0) {
        AUTIL_LOG(ERROR, "can not find match unit for [%s]", logicalDir.c_str());
        return FSEC_NOENT;
    }
    Unit& unit = _units[unitId];
    set<string> dataFileSet;
    set<string> uselessFileSet;
    string recoverMetaPath;
    VersionedPackageFileMeta::Recognize(_description, checkpoint, physicalFileListHint, dataFileSet, uselessFileSet,
                                        recoverMetaPath);
    AUTIL_LOG(INFO, "recognize recover info from [%s], find [%lu] data file, [%lu] useless file",
              recoverMetaPath.c_str(), dataFileSet.size(), uselessFileSet.size());
    // clean all files when checkpoint < 0
    if (checkpoint < 0) {
        uselessFileSet.insert(dataFileSet.begin(), dataFileSet.end());
        if (!recoverMetaPath.empty()) {
            uselessFileSet.insert(recoverMetaPath);
        }
        CleanFromEntryTable(logicalDir, uselessFileSet);
        return CleanData(unit, uselessFileSet, FenceContext::NoFence());
    }
    // no matched package file meta
    if (recoverMetaPath.empty()) {
        AUTIL_LOG(ERROR,
                  "recover package file meta failed, "
                  "can not find meta [%s] with metaId [%d] in [%s/%s]",
                  _description.c_str(), checkpoint, _rootPath.c_str(), logicalDir.c_str());
        return FSEC_ERROR;
    }
    // has matched package file meta
    std::string packageMetaFilePath = PathUtil::JoinPath(logicalDir, recoverMetaPath);
    metaRet = _entryTable->GetEntryMetaMayLazy(packageMetaFilePath);
    if (metaRet.ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "recover package file meta failed, can't find entryMeta [%s] in entryTable",
                  packageMetaFilePath.c_str());
        return FSEC_ERROR;
    }
    EntryMeta meta = metaRet.result;
    VersionedPackageFileMeta packageMeta;
    FSResult<int64_t> ret = packageMeta.Load(meta.GetFullPhysicalPath());
    RETURN_IF_FS_ERROR(ret.ec, "");
    _entryTable->UpdatePackageMetaFile(meta.GetFullPhysicalPath(), ret.result);
    // 现在Merge
    // Package的Recover，是不展开PackgeMeta的，这里有个假定是，Merge按字段Merge，Recover后的新任务（字段Merge）不依赖Recover前的文件产出，即字段间隔离。
    // RecoverEntryTable(logicalDir, PathUtil::JoinPath(_rootPath, logicalDir), packageMeta);
    for (auto iter = packageMeta.Begin(); iter != packageMeta.End(); iter++) {
        unit.metas.insert(make_pair(unit.rootPath + "/" + iter->GetFilePath(), *iter));
    }
    const vector<string>& fileNames = packageMeta.GetPhysicalFileNames();
    const vector<string>& fileTags = packageMeta.GetPhysicalFileTags();
    assert(fileNames.size() == fileTags.size());
    for (size_t i = 0; i < fileNames.size(); ++i) {
        unit.physicalStreams.push_back(
            make_tuple(fileNames[i], std::shared_ptr<BufferedFileOutputStream>(), fileTags[i]));
    }
    unit.nextMetaId = packageMeta.GetVersionId() + 1;

    set<string> fileNameSet(fileNames.begin(), fileNames.end());
    set_difference(dataFileSet.begin(), dataFileSet.end(), fileNameSet.begin(), fileNameSet.end(),
                   inserter(uselessFileSet, uselessFileSet.end()));
    CleanFromEntryTable(logicalDir, uselessFileSet);
    return CleanData(unit, uselessFileSet, FenceContext::NoFence());
}

void PackageDiskStorage::CleanFromEntryTable(const std::string& logicalDir,
                                             const std::set<std::string>& fileNames) noexcept
{
    AUTIL_LOG(INFO, "recover prepare to remove [%lu] from entryTable, files: [%s]", fileNames.size(),
              StringUtil::toString(fileNames.begin(), fileNames.end()).c_str());
    std::vector<string> erasedFiles, afterErasedFiles;
    erasedFiles.reserve(fileNames.size());
    for (const std::string& file : fileNames) {
        std::string logicalPath = PathUtil::JoinPath(logicalDir, file);
        erasedFiles.push_back(logicalPath);
        if (_entryTable->IsExist(logicalPath)) {
            _entryTable->Delete(logicalPath);
        }
    }
    std::vector<EntryMeta> metas = _entryTable->ListDir("", /*recursive=*/true);
    for (const auto& meta : metas) {
        auto path = meta.GetLogicalPath();
        afterErasedFiles.push_back(path);
    }
    AUTIL_LOG(INFO, "erased files: [%s], after [%s]",
              StringUtil::toString(erasedFiles.begin(), erasedFiles.end()).c_str(),
              StringUtil::toString(afterErasedFiles.begin(), afterErasedFiles.end()).c_str());
}

ErrorCode PackageDiskStorage::CleanData(const Unit& unit, const set<string>& fileNames,
                                        FenceContext* fenceContext) noexcept
{
    AUTIL_LOG(INFO, "recover prepare to remove [%lu] files: [%s]", fileNames.size(),
              StringUtil::toString(fileNames.begin(), fileNames.end()).c_str());
    string absPath = JoinRoot(unit.rootPath);
    for (const auto& iter : fileNames) {
        ErrorCode ec =
            FslibWrapper::DeleteFile(PathUtil::JoinPath(absPath, iter), DeleteOption::Fence(fenceContext, true)).Code();
        if (ec != FSEC_OK) {
            return ec;
        }
    }
    return FSEC_OK;
}

tuple<ErrorCode, uint32_t, std::shared_ptr<BufferedFileOutputStream>>
PackageDiskStorage::AllocatePhysicalStream(int32_t unitId, const string& logicalFilePath) noexcept
{
    ScopedLock lock(_lock);
    assert(unitId >= 0 && (size_t)unitId < _units.size());
    return DoAllocatePhysicalStream(_units[unitId], logicalFilePath);
}

tuple<ErrorCode, uint32_t, std::shared_ptr<BufferedFileOutputStream>>
PackageDiskStorage::DoAllocatePhysicalStream(Unit& unit, const std::string& logicalFilePath) noexcept
{
    string tag = ExtractTag(logicalFilePath);
    auto freeIt = unit.freePhysicalFileIds.find(tag);
    if (freeIt == unit.freePhysicalFileIds.end()) {
        freeIt = unit.freePhysicalFileIds.insert(freeIt, make_pair(tag, queue<uint32_t>()));
    }
    if (freeIt->second.empty()) {
        std::shared_ptr<BufferedFileOutputStream> stream(new BufferedFileOutputStream(false));
        uint32_t fileId = unit.physicalStreams.size();
        string physicalFileName = VersionedPackageFileMeta::GetPackageDataFileName(_description, fileId);
        string physicalFilePath = PathUtil::JoinPath(JoinRoot(unit.rootPath), physicalFileName);
        AUTIL_LOG(INFO, "create file[%s]", physicalFilePath.c_str());
        auto ec = stream->Open(physicalFilePath, -1);
        if (ec != FSEC_OK) {
            AUTIL_LOG(INFO, "open stream[%s] failed", physicalFilePath.c_str());
            return {ec, -1, std::shared_ptr<BufferedFileOutputStream>()};
        }
        unit.physicalStreams.push_back(make_tuple(physicalFileName, stream, tag));
        return {FSEC_OK, fileId, std::get<1>(unit.physicalStreams.back())};
    }
    uint32_t fileId = freeIt->second.front();
    assert(fileId < unit.physicalStreams.size());
    const auto& stream = std::get<1>(unit.physicalStreams[fileId]);
    freeIt->second.pop();
    size_t paddingLen = ((stream->GetLength() + _fileAlignSize - 1) & (~(_fileAlignSize - 1))) - stream->GetLength();
    vector<char> paddingBuffer(paddingLen, 0);
    auto [ec, _] = stream->Write(paddingBuffer.data(), paddingLen);
    if (ec != FSEC_OK) {
        return {ec, -1, std::shared_ptr<BufferedFileOutputStream>()};
    }
    return {FSEC_OK, fileId, stream};
}

ErrorCode PackageDiskStorage::StoreLogicalFile(int32_t unitId, const string& logicalFilePath,
                                               unique_ptr<FileBuffer> buffer) noexcept
{
    ScopedLock lock(_lock);
    _isDirty = true;
    _units[unitId].unflushedBuffers.emplace_back(UnflushedFile {logicalFilePath, std::move(buffer)});
    return FSEC_OK;
}

ErrorCode PackageDiskStorage::StoreLogicalFile(int32_t unitId, const string& logicalFilePath, uint32_t physicalFileId,
                                               size_t fileLength) noexcept
{
    string logicalRelativePath = logicalFilePath;
    ScopedLock lock(_lock);
    // eg:            /partition_0_65535/segment_2_level_1/index
    // unit.rootPath: /partition_0_65535/segment_2_level_1
    // after substr:  index
    Unit& unit = _units[unitId];
    InnerFileMeta meta(logicalRelativePath.substr(unit.rootPath.size() + 1), false);
    if (physicalFileId >= unit.physicalStreams.size()) {
        AUTIL_LOG(ERROR, "store logicalFile [%s] failed", logicalFilePath.c_str());
        return FSEC_ERROR;
    }
    const auto& stream = std::get<1>(unit.physicalStreams[physicalFileId]);
    const std::string& tag = std::get<2>(unit.physicalStreams[physicalFileId]);
    assert(stream->GetLength() >= fileLength);
    size_t offset = stream->GetLength() - fileLength;
    meta.Init(offset, fileLength, physicalFileId);
    unit.metas.insert(make_pair(logicalRelativePath, meta));
    unit.freePhysicalFileIds[tag].push(physicalFileId);
    return FSEC_OK;
}

FSResult<std::future<bool>> PackageDiskStorage::Sync() noexcept
{
    std::promise<bool> p;
    auto f = p.get_future();
    p.set_value(true);

    RETURN2_IF_FS_ERROR(FlushBuffers(), std::move(f), "FlushBuffers failed");
    return {FSEC_OK, std::move(f)};
}

// need use DirectoryMerger::MergePackageFiles mv to final package file meta
FSResult<int32_t> PackageDiskStorage::CommitPackage() noexcept
{
    RETURN2_IF_FS_ERROR(FlushBuffers(), -1, "CommitPackage failed when FlushBuffers");

    int32_t metaId = 0;
    ScopedLock lock(_lock);
    for (const Unit& unit : _units) {
        metaId = max(metaId, unit.nextMetaId);
    }
    for (Unit& unit : _units) {
        unit.nextMetaId = metaId + 1;
        VersionedPackageFileMeta packageFileMeta(metaId);
        packageFileMeta.SetFileAlignSize(_fileAlignSize);
        for (auto& tuple : unit.physicalStreams) {
            string& fileName = std::get<0>(tuple);
            auto& stream = std::get<1>(tuple);
            string& fileTag = std::get<2>(tuple);
            if (stream) {
                RETURN2_IF_FS_ERROR(stream->Close(), -1, "Close stream[%s] failed", fileName.c_str());
                stream.reset();
            }
            packageFileMeta.AddPhysicalFile(fileName, fileTag);
        }
        unit.freePhysicalFileIds.clear();
        assert(unit.unflushedBuffers.empty());

        string packageFileRootPhysicalPath = JoinRoot(unit.rootPath);
        string packageFilePhysicalPath = PathUtil::JoinPath(packageFileRootPhysicalPath, PACKAGE_FILE_PREFIX);
        const string& packageMetaFileName = VersionedPackageFileMeta::GetPackageMetaFileName(_description, metaId);
        string packageMetaFilePhysicalPath = PathUtil::JoinPath(packageFileRootPhysicalPath, packageMetaFileName);
        for (const auto& iter : unit.metas) {
            packageFileMeta.AddInnerFile(iter.second);
            string packageDataFilePhysicalPath =
                PackageFileMeta::GetPackageFileDataPath(packageFilePhysicalPath,
                                                        /*description=*/_description,
                                                        /*dataFileIdx=*/iter.second.GetDataFileIdx());
            if (iter.second.IsDir()) {
                _entryTable->UpdatePackageDataFile(/*logicalPath=*/iter.first, packageMetaFilePhysicalPath,
                                                   /*offset=*/0);
            } else {
                _entryTable->UpdatePackageDataFile(/*logicalPath=*/iter.first, packageDataFilePhysicalPath,
                                                   /*offset=*/iter.second.GetOffset());
            }
        }
        auto [ec, packageMetaFileLength] =
            packageFileMeta.Store(packageFileRootPhysicalPath, _description, FenceContext::NoFence());
        if (unlikely(ec != FSEC_OK)) {
            AUTIL_LOG(ERROR, "store package file meta [%s] failed", packageFileRootPhysicalPath.c_str());
            return {ec, -1};
        }
        _entryTable->UpdatePackageMetaFile(packageMetaFilePhysicalPath, /*length=*/packageMetaFileLength);
        const std::vector<std::string>& packageDataFileNames = packageFileMeta.GetPhysicalFileNames();
        const std::vector<size_t>& packageDataFileLengths = packageFileMeta.GetPhysicalFileLengths();
        assert(packageDataFileNames.size() == packageDataFileLengths.size());
        for (size_t i = 0; i < packageDataFileNames.size(); ++i) {
            _entryTable->UpdatePackageMetaFile(PathUtil::JoinPath(packageFileRootPhysicalPath, packageDataFileNames[i]),
                                               /*length=*/packageDataFileLengths[i]);
        }
    }
    AUTIL_LOG(INFO, "commit [%lu] unit, metaId [%d], description [%s]", _units.size(), metaId, _description.c_str());
    return {FSEC_OK, metaId};
}

std::string PackageDiskStorage::ExtractTag(const string& logicalFilePath) const noexcept
{
    if (_tagFunction) {
        return _tagFunction(logicalFilePath);
    }
    return "";
}

std::string PackageDiskStorage::DebugString() const noexcept
{
    std::stringstream ss;
    ss << "Root Path: " << _rootPath << endl;
    for (const auto& unit : _units) {
        ss << "seg root path: " << unit.rootPath << endl;
        for (const auto& iter : unit.metas) {
            ss << "file path: " << iter.second.GetFilePath() << " offset: " << iter.second.GetOffset()
               << " length: " << iter.second.GetLength() << " isDir: " << iter.second.IsDir() << endl;
        }
    }
    return ss.str();
}

}} // namespace indexlib::file_system

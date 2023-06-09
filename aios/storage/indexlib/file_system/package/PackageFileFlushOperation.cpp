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
#include "indexlib/file_system/package/PackageFileFlushOperation.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PackageFileFlushOperation);

PackageFileFlushOperation::PackageFileFlushOperation(const FlushRetryStrategy& flushRetryStrategy,
                                                     const std::string& packageDirPath,
                                                     const PackageFileMetaPtr& packageFileMeta,
                                                     const vector<std::shared_ptr<FileNode>>& fileNodeVec,
                                                     const vector<WriterOption>& writerOptionVec)
    : FileFlushOperation(flushRetryStrategy)
    , _packageDirPath(packageDirPath)
    , _packageFilePath(util::PathUtil::JoinPath(packageDirPath, std::string(PACKAGE_FILE_PREFIX)))
    , _packageFileMeta(packageFileMeta)
    , _innerFileNodes(fileNodeVec)
{
    Init(fileNodeVec, writerOptionVec);
}

PackageFileFlushOperation::~PackageFileFlushOperation() {}

void PackageFileFlushOperation::Init(const vector<std::shared_ptr<FileNode>>& fileNodeVec,
                                     const vector<WriterOption>& writerOptionVec)
{
    assert(_packageFileMeta);
    assert(fileNodeVec.size() == writerOptionVec.size());
    _flushFileNodes.reserve(fileNodeVec.size());

    for (size_t i = 0; i < fileNodeVec.size(); ++i) {
        std::shared_ptr<FileNode> flushFileNode = fileNodeVec[i];
        if (writerOptionVec[i].copyOnDump) {
            // already cloned in PackageMemStorage::StoreFile
            // AUTIL_LOG(INFO, "CopyFileNode for dump %s", fileNodeVec[i]->DebugString().c_str());
            // flushFileNode.reset(fileNodeVec[i]->Clone());
            _flushMemoryUse += flushFileNode->GetLength();
        }
        _flushFileNodes.push_back(flushFileNode);
    }
}

ErrorCode PackageFileFlushOperation::DoExecute() noexcept
{
    string packageFileMetaPath = GetMetaFilePath();
    auto [ec, exist] = FslibWrapper::IsExist(packageFileMetaPath);
    RETURN_IF_FS_ERROR(ec, "is exist failed, path [%s]", packageFileMetaPath.c_str());
    if (exist) {
        AUTIL_LOG(ERROR, "file [%s] already exist.", packageFileMetaPath.c_str());
        return FSEC_EXIST;
    }

    RETURN_IF_FS_ERROR(StoreDataFile(), "");
    RETURN_IF_FS_ERROR(StoreMetaFile(), "");
    MakeFlushedFileNotDirty();
    return FSEC_OK;
}

void PackageFileFlushOperation::CleanDirtyFile() noexcept
{
    auto ec = FslibWrapper::DeleteFile(GetDataFilePath(), DeleteOption::NoFence(true)).Code();
    if (ec != FSEC_OK) {
        AUTIL_LOG(INFO, "clean package data file[%s] fail", GetDataFilePath().c_str());
    }
    ec = FslibWrapper::DeleteFile(GetMetaFilePath(), DeleteOption::NoFence(true)).Code();
    if (ec != FSEC_OK) {
        AUTIL_LOG(INFO, "clean package data file[%s] fail", GetMetaFilePath().c_str());
    }
}

string PackageFileFlushOperation::GetDataFilePath()
{
    return PackageFileMeta::GetPackageFileDataPath(_packageFilePath, "", 0);
}

string PackageFileFlushOperation::GetMetaFilePath() { return _packageFilePath + PACKAGE_FILE_META_SUFFIX; }

ErrorCode PackageFileFlushOperation::StoreDataFile() noexcept
{
    string physicalPath = GetDataFilePath();
    auto [ec, dataFile] = FslibWrapper::OpenFile(physicalPath, fslib::WRITE);
    RETURN_IF_FS_ERROR(ec, "open file [%s] failed", physicalPath.c_str());
    size_t dataFileLen = 0;
    size_t innerFileIdx = 0;
    vector<char> paddingBuffer(_packageFileMeta->GetFileAlignSize(), 0);
    InnerFileMeta::Iterator iter = _packageFileMeta->Begin();
    for (; iter != _packageFileMeta->End(); iter++) {
        const InnerFileMeta& innerFileMeta = *iter;
        if (innerFileMeta.IsDir()) {
            continue;
        }

        size_t paddingLen = innerFileMeta.GetOffset() - dataFileLen;
        RETURN_IF_FS_ERROR(dataFile->NiceWrite(paddingBuffer.data(), paddingLen), "");
        dataFileLen += paddingLen;

        const std::shared_ptr<FileNode>& innerFileNode = _flushFileNodes[innerFileIdx++];
        assert(innerFileNode->GetType() == FSFT_MEM);

        void* data = innerFileNode->GetBaseAddress();
        size_t dataLen = innerFileNode->GetLength();
        assert(innerFileMeta.GetLength() == dataLen);
        RETURN_IF_FS_ERROR(SplitWrite(dataFile, data, dataLen), "");
        dataFileLen += dataLen;
    }
    assert(innerFileIdx == _flushFileNodes.size());
    RETURN_IF_FS_ERROR(dataFile->Close().Code(), "");
    AUTIL_LOG(INFO, "flush file[%s], len=%luB", physicalPath.c_str(), dataFileLen);
    return FSEC_OK;
}

ErrorCode PackageFileFlushOperation::StoreMetaFile() noexcept
{
    // use @Store to dump atomic, avoid branch fs mount inconsistent package data and meta
    auto ec = _packageFileMeta->Store(_packageDirPath, FenceContext::NoFence());
    size_t length = 0;
    RETURN_IF_FS_ERROR(ec, "failed to store package meta, path [%s]", GetMetaFilePath().c_str());
    RETURN_IF_FS_ERROR(_packageFileMeta->GetMetaStrLength(&length),
                       "failed to get package meta content length, path [%s]", GetMetaFilePath().c_str());
    AUTIL_LOG(INFO, "flush file[%s], len=%zuB", GetMetaFilePath().c_str(), length);
    return FSEC_OK;
}

void PackageFileFlushOperation::MakeFlushedFileNotDirty() noexcept
{
    for (size_t i = 0; i < _innerFileNodes.size(); i++) {
        _innerFileNodes[i]->SetDirty(false);
    }
}

void PackageFileFlushOperation::GetFileNodePaths(FileList& fileList) const noexcept
{
    for (size_t i = 0; i < _innerFileNodes.size(); i++) {
        fileList.push_back(_innerFileNodes[i]->GetLogicalPath());
    }
}
}} // namespace indexlib::file_system

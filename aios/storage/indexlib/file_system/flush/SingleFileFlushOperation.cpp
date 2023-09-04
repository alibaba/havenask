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
#include "indexlib/file_system/flush/SingleFileFlushOperation.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SingleFileFlushOperation);

SingleFileFlushOperation::SingleFileFlushOperation(const FlushRetryStrategy& flushRetryStrategy,
                                                   const std::shared_ptr<FileNode>& fileNode,
                                                   const WriterOption& writerOption)
    : FileFlushOperation(flushRetryStrategy)
    , _destPath(fileNode->GetPhysicalPath())
    , _fileNode(fileNode)
    , _writerOption(writerOption)
{
    assert(_fileNode);
    if (writerOption.copyOnDump) {
        AUTIL_LOG(INFO, "CopyFileNode for dump %s", _fileNode->DebugString().c_str());
        _flushFileNode.reset(_fileNode->Clone());
        _flushMemoryUse += _flushFileNode->GetLength();
    } else {
        _flushFileNode = _fileNode;
    }
}

SingleFileFlushOperation::~SingleFileFlushOperation() {}

ErrorCode SingleFileFlushOperation::DoExecute() noexcept
{
    if (_writerOption.atomicDump) {
        RETURN_IF_FS_ERROR(AtomicStore(_destPath), "");
    } else {
        RETURN_IF_FS_ERROR(Store(_destPath), "");
    }

    _fileNode->SetDirty(false);
    return FSEC_OK;
}

void SingleFileFlushOperation::CleanDirtyFile() noexcept
{
    if (_writerOption.atomicDump) {
        // atomic store do not clean file ???
        return;
    }
    auto ec = FslibWrapper::DeleteDir(_destPath, DeleteOption::NoFence(true)).Code();
    if (ec != FSEC_OK) {
        AUTIL_LOG(INFO, "clean file[%s] fail", _destPath.c_str());
    }
}

FSResult<void> SingleFileFlushOperation::Store(const string& filePath) noexcept
{
    assert(_flushFileNode);
    assert(_flushFileNode->GetType() == FSFT_MEM);
    void* data = _flushFileNode->GetBaseAddress();
    size_t dataLen = _flushFileNode->GetLength();

    string path = filePath;

    // will make parent dir, so we do not check dir exists
    auto [ec, file] = FslibWrapper::OpenFile(path, fslib::WRITE, false, dataLen);
    if (ec != FSEC_OK) {
        return ec;
    }
    if (dataLen > 0) {
        assert(data);
        auto ec = SplitWrite(file, data, dataLen);
        if (ec != FSEC_OK) {
            return ec;
        }
    }
    AUTIL_LOG(INFO, "flush file[%s], len [%lu] B", path.c_str(), dataLen);
    return file->Close();
}

FSResult<void> SingleFileFlushOperation::AtomicStore(const string& filePath) noexcept
{
    auto [ec0, exist0] = FslibWrapper::IsExist(filePath);
    if (ec0 != FSEC_OK) {
        return ec0;
    } else if (exist0) {
        AUTIL_LOG(ERROR, "file [%s] already exist.", filePath.c_str());
        return FSEC_EXIST;
    }
    string tempFilePath = filePath + TEMP_FILE_SUFFIX;
    auto [ec1, exist1] = FslibWrapper::IsExist(tempFilePath);
    if (ec1 != FSEC_OK) {
        return ec1;
    } else if (exist1) {
        AUTIL_LOG(WARN, "Remove temp file: %s.", tempFilePath.c_str());
        RETURN_IF_FS_ERROR(FslibWrapper::DeleteFile(tempFilePath, DeleteOption::NoFence(false)).Code(), "");
    }

    RETURN_IF_FS_ERROR(Store(tempFilePath), "");
    RETURN_IF_FS_ERROR(FslibWrapper::Rename(tempFilePath, filePath).Code(), "");
    return FSEC_OK;
}

void SingleFileFlushOperation::GetFileNodePaths(FileList& fileList) const noexcept
{
    fileList.push_back(_fileNode->GetLogicalPath());
}
}} // namespace indexlib::file_system

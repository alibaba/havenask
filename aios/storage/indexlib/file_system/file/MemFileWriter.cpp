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
#include "indexlib/file_system/file/MemFileWriter.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/util/IoExceptionController.h"
#include "indexlib/util/MmapPool.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace file_system {
struct WriterOption;
}} // namespace indexlib::file_system

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, MemFileWriter);

const bool MemFileWriter::NEED_SKIP_MMAP = autil::EnvUtil::getEnv("INDEXLIB_SKIP_MMAP_FOR_MEMFILE", false);

MemFileWriter::MemFileWriter(const util::BlockMemoryQuotaControllerPtr& memController, Storage* storage,
                             const WriterOption& writerOption,
                             FileWriterImpl::UpdateFileSizeFunc&& updateFileSizeFunc) noexcept
    : FileWriterImpl(writerOption, std::move(updateFileSizeFunc))
    , _storage(storage)
    , _sliceArray(NULL)
    , _memController(memController)
    , _length(0)
    , _isClosed(true)
{
    assert(storage);
}

MemFileWriter::~MemFileWriter() noexcept
{
    if (!_isClosed && _writerOption.openType != FSOT_MEM) {
        assert(_writerOption.openType == FSOT_UNKNOWN);
        AUTIL_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]", _physicalPath.c_str(),
                  util::IoExceptionController::HasFileIOException());
        assert(util::IoExceptionController::HasFileIOException());
    }
    DELETE_AND_SET_NULL(_sliceArray);
}

ErrorCode MemFileWriter::DoOpen() noexcept
{
    assert(!_memFileNode);
    if (_writerOption.fileLength >= 0) {
        _memFileNode.reset(
            new MemFileNode(_writerOption.fileLength, false, LoadConfig(), _memController, {}, NEED_SKIP_MMAP));
        RETURN_IF_FS_ERROR(_memFileNode->Open(_logicalPath, _physicalPath, FSOT_MEM, -1), "open failed");
        _memFileNode->Resize(_writerOption.fileLength);
        _memFileNode->SetDirty(true);
        _memFileNode->SetMetricGroup(_writerOption.metricGroup);
        RETURN_IF_FS_ERROR(_storage->StoreWhenNonExist(_memFileNode), "");
    } else {
        DELETE_AND_SET_NULL(_sliceArray);
        _sliceArray = NEED_SKIP_MMAP ? new SliceArray(SLICE_LEN, SLICE_NUM, new autil::mem_pool::Pool, true)
                                     : new SliceArray(SLICE_LEN, SLICE_NUM, new MmapPool, true);
        _memFileNode.reset();
        _length = 0;
    }
    _isClosed = false;
    return FSEC_OK;
}

FSResult<void> MemFileWriter::ReserveFile(size_t reservedSize) noexcept
{
    if (_physicalPath.empty() || _length > 0 || reservedSize == 0) {
        // slice array already have data, not use _memFileNode
        return FSEC_OK;
    }

    DELETE_AND_SET_NULL(_sliceArray);
    _memFileNode.reset(new MemFileNode(reservedSize, false, LoadConfig(), _memController, {}, NEED_SKIP_MMAP));
    RETURN_IF_FS_ERROR(_memFileNode->Open(_logicalPath, _physicalPath, FSOT_MEM, -1), "open failed");
    _memFileNode->SetDirty(true);
    _memFileNode->SetMetricGroup(_writerOption.metricGroup);
    return FSEC_OK;
}

FSResult<void> MemFileWriter::Truncate(size_t truncateSize) noexcept
{
    assert(_memFileNode);
    assert(!_sliceArray);
    _storage->Truncate(_memFileNode->GetLogicalPath(), truncateSize);
    _memFileNode->Resize(truncateSize);
    _length = _memFileNode->GetLength();
    _updateFileSizeFunc(truncateSize);
    return FSEC_OK;
}

FSResult<size_t> MemFileWriter::Write(const void* buffer, size_t length) noexcept
{
    if (_memFileNode) {
        size_t fileNodeLength = _memFileNode->GetLength();
        size_t copyLength = fileNodeLength > _length ? std::min(fileNodeLength - _length, length) : 0;
        if (copyLength > 0) {
            memcpy((char*)_memFileNode->GetBaseAddress() + _length, buffer, copyLength);
            _length += copyLength;
        } else if (length - copyLength > 0) {
            auto [ec, writeLen] = _memFileNode->Write(buffer, length - copyLength);
            RETURN2_IF_FS_ERROR(ec, writeLen, "Write failed");
            _length += (length - copyLength);
            assert(_length == _memFileNode->GetLength());
        }
        return {FSEC_OK, length};
    }

    assert(_sliceArray);
    RETURN2_IF_FS_EXCEPTION(_sliceArray->SetList(_length, (const char*)buffer, length), 0, "SetList failed");
    _length += length;
    assert((int64_t)_length == _sliceArray->GetDataSize());
    return {FSEC_OK, length};
}

size_t MemFileWriter::GetLength() const noexcept { return _length; }

ErrorCode MemFileWriter::DoClose() noexcept
{
    if (_isClosed) {
        return FSEC_OK;
    }
    _isClosed = true;
    if (!_memFileNode) {
        auto [ec, fileNode] = CreateFileNodeFromSliceArray();
        RETURN_IF_FS_ERROR(ec, "CreateFileNodeFromSliceArray failed");
        _memFileNode = fileNode;
    }
    if (!_memFileNode) {
        return FSEC_OK;
    }
    _length = _memFileNode->GetLength();
    if (_writerOption.noDump) {
        _memFileNode->SetDirty(false);
        return FSEC_OK;
    }
    assert(!_sliceArray);
    auto ec = _storage->StoreFile(_memFileNode, _writerOption);
    RETURN_IF_FS_ERROR(ec, "store file [%s] failed", _memFileNode->DebugString().c_str());
    assert(_memFileNode.use_count() > 1);
    _memFileNode.reset();
    return FSEC_OK;
}

FSResult<std::shared_ptr<MemFileNode>> MemFileWriter::CreateFileNodeFromSliceArray() noexcept
{
    if (!_sliceArray) {
        return {FSEC_OK, std::shared_ptr<MemFileNode>()};
    }

    std::shared_ptr<MemFileNode> fileNode(
        new MemFileNode(_length, false, LoadConfig(), _memController, {}, NEED_SKIP_MMAP));
    RETURN2_IF_FS_ERROR(fileNode->Open(_logicalPath, _physicalPath, FSOT_MEM, -1), std::shared_ptr<MemFileNode>(),
                        "open failed");
    fileNode->SetDirty(true);
    fileNode->SetMetricGroup(_writerOption.metricGroup);

    assert((int64_t)_length == _sliceArray->GetDataSize());
    for (size_t i = 0; i < _sliceArray->GetSliceNum(); ++i) {
        char* addr = nullptr;
        RETURN2_IF_FS_EXCEPTION((addr = _sliceArray->GetSlice(i)), std::shared_ptr<MemFileNode>(), "GetSlice failed");
        RETURN2_IF_FS_ERROR(fileNode->Write(addr, _sliceArray->GetSliceDataLen(i)).Code(),
                            std::shared_ptr<MemFileNode>(), "Write failed");
    }
    DELETE_AND_SET_NULL(_sliceArray);
    return {FSEC_OK, fileNode};
}

void* MemFileWriter::GetBaseAddress() noexcept
{
    assert(_length == _memFileNode->GetLength()); // call truncate first
    return _memFileNode ? _memFileNode->GetBaseAddress() : NULL;
}
}} // namespace indexlib::file_system

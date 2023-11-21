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
#include "indexlib/file_system/stream/BlockFileStream.h"

#include "autil/memory.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BlockFileStream);

using file_system::ReadOption;

BlockFileStream::BlockFileStream(const std::shared_ptr<file_system::FileReader>& fileReader, bool supportConcurrency)
    : FileStream(fileReader->GetLength(), supportConcurrency)
    , _blockFileNode(nullptr)
    , _fileReader(fileReader)
    , _offset(0)
{
    assert(fileReader->GetFileNode()->GetType() == file_system::FSFT_BLOCK);
    _blockFileNode = (file_system::BlockFileNode*)(fileReader->GetFileNode().get());
}

BlockFileStream::~BlockFileStream() {}

FSResult<size_t> BlockFileStream::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_blockFileNode);
    auto accessor = _blockFileNode->GetAccessor();
    if (!_supportConcurrency && accessor->GetBlockCount(offset, length) == 1) {
        if (!_currentHandle.GetData() || offset < _currentHandle.GetOffset() ||
            offset >= _currentHandle.GetOffset() + _currentHandle.GetDataSize()) {
            if (offset + length > accessor->GetFileLength()) {
                AUTIL_LOG(ERROR,
                          "read file [%s] out of range, offset: [%lu], "
                          "read length: [%lu], file length: [%lu]",
                          _blockFileNode->DebugString().c_str(), offset, length, accessor->GetFileLength());
                return {FSEC_BADARGS, length};
            }
            try {
                auto ret = accessor->GetBlock(offset, _currentHandle, &option);
                if (!ret.OK()) {
                    AUTIL_LOG(ERROR,
                              "read file [%s] io exception, ec:[%d], offset: [%lu], "
                              "read length: [%lu], file length: [%lu]",
                              _blockFileNode->DebugString().c_str(), ret.Code(), offset, length,
                              accessor->GetFileLength());
                    return {FSEC_BADARGS, length};
                }
            } catch (...) {
                AUTIL_LOG(ERROR,
                          "read file [%s] io exception , offset: [%lu], "
                          "read length: [%lu], file length: [%lu]",
                          _blockFileNode->DebugString().c_str(), offset, length, accessor->GetFileLength());
                return {FSEC_BADARGS, length};
            }
        }
        ::memcpy(buffer, _currentHandle.GetData() + offset - _currentHandle.GetOffset(), length);
        return {FSEC_OK, length};
    }
    return _blockFileNode->Read(buffer, length, offset, option);
}

future_lite::Future<FSResult<size_t>> BlockFileStream::ReadAsync(void* buffer, size_t length, size_t offset,
                                                                 file_system::ReadOption option) noexcept(false)
{
    assert(_blockFileNode);

    auto accessor = _blockFileNode->GetAccessor();
    if (!_supportConcurrency && accessor->GetBlockCount(offset, length) == 1) {
        if (!_currentHandle.GetData() || offset < _currentHandle.GetOffset() ||
            offset >= _currentHandle.GetOffset() + _currentHandle.GetDataSize()) {
            if (offset + length > accessor->GetFileLength()) {
                AUTIL_LOG(ERROR,
                          "read file [%s] out of range, offset: [%lu], "
                          "read length: [%lu], file length: [%lu]",
                          _blockFileNode->DebugString().c_str(), offset, length, accessor->GetFileLength());
                return future_lite::makeReadyFuture<FSResult<size_t>>({FSEC_OK, 0});
            }
            return accessor->GetBlockAsync(offset, option)
                .thenValue(
                    [this, buffer, offset, length](FSResult<util::BlockHandle>&& ret) mutable -> FSResult<size_t> {
                        RETURN2_IF_FS_ERROR(ret.Code(), 0, "GetBlockAsync failed");
                        _currentHandle = std::move(ret.Value());
                        ::memcpy(buffer, _currentHandle.GetData() + offset - _currentHandle.GetOffset(), length);
                        return FSResult<size_t> {FSEC_OK, length};
                    });
        }
        ::memcpy(buffer, _currentHandle.GetData() + offset - _currentHandle.GetOffset(), length);
        return future_lite::makeReadyFuture<FSResult<size_t>>({FSEC_OK, length});
    } else {
        return _blockFileNode->ReadAsync(buffer, length, offset, option);
    }
    assert(false);
    return future_lite::makeReadyFuture<FSResult<size_t>>({FSEC_OK, 0});
}

future_lite::coro::Lazy<std::vector<file_system::FSResult<size_t>>>
BlockFileStream::BatchRead(file_system::BatchIO& batchIO, file_system::ReadOption option) noexcept
{
    co_return co_await _fileReader->BatchRead(batchIO, option);
}

std::shared_ptr<FileStream> BlockFileStream::CreateSessionStream(autil::mem_pool::Pool* pool) const
{
    return FileStreamCreator::CreateFileStream(_fileReader, pool);
}

std::string BlockFileStream::DebugString() const { return _fileReader->DebugString(); }

} // namespace indexlib::file_system

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
#include "indexlib/file_system/file/BufferedFileReader.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/ThreadPool.h"
#include "indexlib/file_system/file/BufferedFileNode.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileWorkItem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

namespace autil {
class WorkItem;
} // namespace autil

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileReader);

BufferedFileReader::BufferedFileReader(const std::shared_ptr<FileNode>& fileNode, uint32_t bufferSize) noexcept
    : _fileNode(fileNode)
    , _fileLength(fileNode->GetLength())
    , _curBlock(-1)
    , _buffer(NULL)
    , _switchBuffer(NULL)
{
    assert(fileNode);
    ResetBufferParam(bufferSize, false);
}

BufferedFileReader::BufferedFileReader(uint32_t bufferSize, bool asyncRead) noexcept
    : _fileNode()
    , _fileLength(-1)
    , _curBlock(-1)
    , _buffer(new FileBuffer(bufferSize))
    , _switchBuffer(NULL)
{
    if (asyncRead) {
        _switchBuffer = new FileBuffer(bufferSize);
        _threadPool = FslibWrapper::GetReadThreadPool();
    }
}

BufferedFileReader::~BufferedFileReader() noexcept
{
    if (_fileNode) {
        [[maybe_unused]] auto ret = Close();
    }
    DELETE_AND_SET_NULL(_buffer);
    DELETE_AND_SET_NULL(_switchBuffer);
}

FSResult<void> BufferedFileReader::Open(const std::string& filePath) noexcept
{
    _fileNode.reset(new BufferedFileNode());
    RETURN_IF_FS_ERROR(_fileNode->Open("LOGICAL_PATH", filePath, FSOT_BUFFERED, -1), "open file node [%s] failed",
                       filePath.c_str());
    _fileLength = _fileNode->GetLength();
    return FSEC_OK;
}

void BufferedFileReader::ResetBufferParam(size_t bufferSize, bool asyncRead) noexcept
{
    DELETE_AND_SET_NULL(_buffer);
    DELETE_AND_SET_NULL(_switchBuffer);
    _buffer = new FileBuffer(bufferSize);
    if (asyncRead) {
        _switchBuffer = new FileBuffer(bufferSize);
        _threadPool = FslibWrapper::GetReadThreadPool();
    } else {
        _threadPool.reset();
    }

    _offset = 0;
    _curBlock = -1;
}

FSResult<size_t> BufferedFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    RETURN2_IF_FS_ERROR(Seek(offset), 0, "Seek failed");
    return Read(buffer, length, option);
}

FSResult<size_t> BufferedFileReader::Read(void* buffer, size_t length, ReadOption option) noexcept
{
    int64_t leftLen = length;
    uint32_t bufferSize = _buffer->GetBufferSize();
    int64_t blockNum = _offset / bufferSize;
    char* cursor = (char*)buffer;
    while (true) {
        if (_curBlock != blockNum) {
            RETURN2_IF_FS_ERROR(LoadBuffer(blockNum, option), (size_t)(cursor - (char*)buffer), "LoadBuffer failed");
        }

        int64_t dataEndOfCurBlock =
            (blockNum + 1) * bufferSize < _fileLength ? (blockNum + 1) * bufferSize : _fileLength;
        int64_t dataLeftInBlock = dataEndOfCurBlock - _offset;
        int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;
        int64_t offsetInBlock = _offset - blockNum * bufferSize;
        memcpy(cursor, _buffer->GetBaseAddr() + offsetInBlock, lengthToCopy);
        cursor += lengthToCopy;
        leftLen -= lengthToCopy;
        _offset += lengthToCopy;
        if (leftLen <= 0) {
            assert(leftLen == 0);
            return {FSEC_OK, length};
        }
        if (_offset >= _fileLength) {
            return {FSEC_OK, (size_t)(cursor - (char*)buffer)};
        }
        blockNum++;
    }
    return {FSEC_OK, 0};
}

ByteSliceList* BufferedFileReader::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "Not support Read with ByteSliceList");
    return NULL;
}

void* BufferedFileReader::GetBaseAddress() const noexcept { return NULL; }

size_t BufferedFileReader::GetLength() const noexcept { return _fileNode->GetLength(); }

const string& BufferedFileReader::GetLogicalPath() const noexcept { return _fileNode->GetLogicalPath(); }

const std::string& BufferedFileReader::GetPhysicalPath() const noexcept { return _fileNode->GetPhysicalPath(); }

FSOpenType BufferedFileReader::GetOpenType() const noexcept { return _fileNode->GetOpenType(); }

FSFileType BufferedFileReader::GetType() const noexcept { return _fileNode->GetType(); }

FSResult<void> BufferedFileReader::Close() noexcept
{
    if (_switchBuffer) {
        _switchBuffer->Wait();
    }

    if (_threadPool) {
        RETURN_IF_FS_EXCEPTION(_threadPool->checkException(), "checkException failed");
        _threadPool.reset();
    }
    return FSEC_OK;
}

FSResult<void> BufferedFileReader::LoadBuffer(int64_t blockNum, ReadOption option) noexcept
{
    assert(_curBlock != blockNum);
    if (_threadPool) {
        return AsyncLoadBuffer(blockNum, option);
    } else {
        return SyncLoadBuffer(blockNum, option);
    }
}

FSResult<void> BufferedFileReader::SyncLoadBuffer(int64_t blockNum, ReadOption option) noexcept
{
    uint32_t bufferSize = _buffer->GetBufferSize();
    int64_t offset = blockNum * bufferSize;
    uint32_t readSize = bufferSize;
    if (offset + readSize > _fileLength) {
        readSize = _fileLength - offset;
    }

    auto [ec, readLen] = DoRead(_buffer->GetBaseAddr(), readSize, offset, option);
    RETURN_IF_FS_ERROR(ec, "DoRead failed");
    if (readLen != readSize) {
        AUTIL_LOG(ERROR, "Read data file failed(%s), offset(%lu), length to read(%u), return len(%lu).",
                  _fileNode->DebugString().c_str(), offset, readSize, readLen);
        return FSEC_ERROR;
    }
    _curBlock = blockNum;
    return FSEC_OK;
}

FSResult<void> BufferedFileReader::AsyncLoadBuffer(int64_t blockNum, ReadOption option) noexcept
{
    assert(_switchBuffer);
    uint32_t bufferSize = _buffer->GetBufferSize();
    if (_curBlock < 0 || (_curBlock != blockNum && _curBlock + 1 != blockNum)) {
        _switchBuffer->Wait(); // do not seek when buffer has not been loaded
        int64_t offset = blockNum * bufferSize;
        auto [ec, readLen] = DoRead(_buffer->GetBaseAddr(), bufferSize, offset, option);
        RETURN_IF_FS_ERROR(ec, "DoRead failed");
        _buffer->SetCursor(readLen);
        PrefetchBlock(blockNum + 1, option);
    } else if (_curBlock + 1 == blockNum) {
        _switchBuffer->Wait();
        std::swap(_buffer, _switchBuffer);
        PrefetchBlock(blockNum + 1, option);
    }
    _curBlock = blockNum;
    return FSEC_OK;
}

FSResult<size_t> BufferedFileReader::DoRead(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_fileNode);
    return _fileNode->Read(buffer, length, offset, option);
}

void BufferedFileReader::PrefetchBlock(int64_t blockNum, ReadOption option) noexcept
{
    size_t offset = blockNum * _buffer->GetBufferSize();
    if (offset >= (size_t)_fileLength) {
        return;
    }

    WorkItem* item = new ReadWorkItem(_fileNode.get(), _switchBuffer, offset, option);
    _switchBuffer->SetBusy();
    if (ThreadPool::ERROR_NONE != _threadPool->pushWorkItem(item)) {
        ThreadPool::dropItemIgnoreException(item);
    }
}
}} // namespace indexlib::file_system

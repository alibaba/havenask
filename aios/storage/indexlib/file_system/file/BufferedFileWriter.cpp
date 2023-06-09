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
#include "indexlib/file_system/file/BufferedFileWriter.h"

#include <cstddef>
#include <functional>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/ThreadPool.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BufferedFileOutputStream.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/file_system/file/FileWorkItem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/IoExceptionController.h"

namespace autil {
class WorkItem;
} // namespace autil

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileWriter);

BufferedFileWriter::BufferedFileWriter(const WriterOption& writerOption,
                                       UpdateFileSizeFunc&& updateFileSizeFunc) noexcept
    : FileWriterImpl(writerOption, std::move(updateFileSizeFunc))
    , _length(0)
    , _isClosed(0)
{
    _buffer = std::make_unique<FileBuffer>(writerOption.bufferSize);
    _switchBuffer = nullptr;
    assert(writerOption.bufferSize > 0);
    if (writerOption.asyncDump) {
        _switchBuffer = std::make_unique<FileBuffer>(writerOption.bufferSize);
        _threadPool = FslibWrapper::GetWriteThreadPool();
    }
}

BufferedFileWriter::~BufferedFileWriter() noexcept
{
    if (!_isClosed) {
        AUTIL_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]", _physicalPath.c_str(),
                  util::IoExceptionController::HasFileIOException());
    }
}

FSResult<void> BufferedFileWriter::ResetBufferParam(size_t bufferSize, bool asyncWrite) noexcept
{
    if (_length > 0) {
        AUTIL_LOG(ERROR, "Not support ResetBufferParam after Write Data");
        return FSEC_NOTSUP;
    }

    _buffer = std::make_unique<FileBuffer>(bufferSize);
    _switchBuffer = nullptr;
    if (asyncWrite) {
        _switchBuffer = std::make_unique<FileBuffer>(bufferSize);
        _threadPool = FslibWrapper::GetWriteThreadPool();
    } else {
        _threadPool.reset();
    }
    return FSEC_OK;
}

string BufferedFileWriter::GetDumpPath() const noexcept
{
    if (_writerOption.atomicDump) {
        return _physicalPath + TEMP_FILE_SUFFIX;
    }
    return _physicalPath;
}

ErrorCode BufferedFileWriter::DoOpen() noexcept
{
    string dumpPath = GetDumpPath();
    _stream.reset(new BufferedFileOutputStream(_writerOption.isAppendMode));
    RETURN_IF_FS_ERROR(_stream->Open(dumpPath, _writerOption.fileLength), "open stream failed");
    _isClosed = false;
    if (_writerOption.isAppendMode) {
        _length = _stream->GetLength();
    }
    return FSEC_OK;
}

FSResult<size_t> BufferedFileWriter::Write(const void* buffer, size_t length) noexcept
{
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true) {
        uint32_t leftLenInBuffer = _buffer->GetBufferSize() - _buffer->GetCursor();
        uint32_t lengthToWrite = leftLenInBuffer < leftLen ? leftLenInBuffer : leftLen;
        _buffer->CopyToBuffer(cursor, lengthToWrite);
        cursor += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0) {
            assert(leftLen == 0);
            _length += length;
            break;
        }
        if (_buffer->GetCursor() == _buffer->GetBufferSize()) {
            RETURN2_IF_FS_ERROR(DumpBuffer(), 0, "DumpBuffer failed");
        }
    }

    return {FSEC_OK, length};
}

size_t BufferedFileWriter::GetLength() const noexcept { return _length; }

ErrorCode BufferedFileWriter::DoClose() noexcept
{
    if (_isClosed) {
        return FSEC_OK;
    }
    _isClosed = true;
    RETURN_IF_FS_ERROR(DumpBuffer(), "dump buffer failed, file[%s]", _physicalPath.c_str());

    if (_switchBuffer) {
        _switchBuffer->Wait();
    }

    RETURN_IF_FS_ERROR(_stream->Close(), "close stream[%s] failed", _physicalPath.c_str());

    if (_writerOption.atomicDump) {
        // trivial writer no need fence
        string dumpPath = GetDumpPath();
        AUTIL_LOG(DEBUG, "Rename temp file[%s] to file[%s].", dumpPath.c_str(), _physicalPath.c_str());
        auto ec = FslibWrapper::Rename(dumpPath, _physicalPath /*no fence*/).Code();
        if (ec == FSEC_EXIST) {
            AUTIL_LOG(WARN, "file [%s] already exist, remove it for renaming purpose", _physicalPath.c_str());
            auto ec = FslibWrapper::DeleteFile(_physicalPath, DeleteOption::NoFence(false)).Code();
            RETURN_IF_FS_ERROR(ec, "Delete file failed: [%s]", _physicalPath.c_str());
            ec = FslibWrapper::Rename(dumpPath, _physicalPath /*no fence*/).Code();
            RETURN_IF_FS_ERROR(ec, "Rename file failed: [%s] => [%s]", dumpPath.c_str(), _physicalPath.c_str());
        } else if (ec != FSEC_OK) {
            RETURN_IF_FS_ERROR(ec, "Rename file failed: [%s] => [%s]", dumpPath.c_str(), _physicalPath.c_str());
        }
    }

    if (_threadPool) {
        RETURN_IF_FS_EXCEPTION(_threadPool->checkException(), "thread pool meet exception, file[%s]",
                               _physicalPath.c_str());
    }
    return FSEC_OK;
}

FSResult<void> BufferedFileWriter::DumpBuffer() noexcept
{
    if (_threadPool) {
        _switchBuffer->Wait();
        _buffer.swap(_switchBuffer);
        WorkItem* item = new WriteWorkItem(_stream.get(), _switchBuffer.get());
        _switchBuffer->SetBusy();
        if (ThreadPool::ERROR_NONE != _threadPool->pushWorkItem(item)) {
            ThreadPool::dropItemIgnoreException(item);
        }
    } else {
        RETURN_IF_FS_ERROR(_stream->Write(_buffer->GetBaseAddr(), _buffer->GetCursor()).Code(), "write stream failed");
        _buffer->SetCursor(0);
    }
    return FSEC_OK;
}

FSResult<bool> BufferedFileWriter::Flush() noexcept
{
    assert(!_threadPool);
    if (unlikely(_threadPool != nullptr)) {
        AUTIL_LOG(ERROR, "asyncDump file [%s] not support Flush()", _physicalPath.c_str());
        return {FSEC_OK, false};
    }
    RETURN2_IF_FS_ERROR(_stream->Write(_buffer->GetBaseAddr(), _buffer->GetCursor()).Code(), false,
                        "write stream[%s] failed", _physicalPath.c_str());
    _buffer->SetCursor(0);

    auto [ec, forceFlush] = _stream->ForceFlush();
    RETURN2_IF_FS_ERROR(ec, false, "ForceFlush[%s] failed", _physicalPath.c_str());
    if (!forceFlush) {
        AUTIL_LOG(ERROR, "async force flush failed, file[%s]", _physicalPath.c_str());
        return {FSEC_OK, false};
    }
    if (_updateFileSizeFunc) {
        _updateFileSizeFunc(GetLength());
    }
    return {FSEC_OK, true};
}
}} // namespace indexlib::file_system

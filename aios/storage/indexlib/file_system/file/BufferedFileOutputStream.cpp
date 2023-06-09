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
#include "indexlib/file_system/file/BufferedFileOutputStream.h"

#include <cstddef>

#include "fslib/common/common_type.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BufferedFileNode.h"
#include "indexlib/file_system/file/FileBuffer.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileOutputStream);

FSResult<void> BufferedFileOutputStream::Open(const std::string& path, int64_t fileLength) noexcept
{
    _path = path;
    _buffer = std::make_unique<FileBuffer>(0);
    if (_isAppendMode || fileLength >= 0) {
        RETURN_IF_FS_ERROR(InitFileNode(fileLength), "InitFileNode failed");
    }
    return FSEC_OK;
}

FSResult<size_t> BufferedFileOutputStream::Write(const void* buffer, size_t length) noexcept
{
    if (_buffer && length <= _buffer->GetFreeSpace()) {
        _buffer->CopyToBuffer((char*)buffer, length);
        return {FSEC_OK, length};
    }
    RETURN2_IF_FS_ERROR(InitFileNode(-1), 0, "InitFileNode failed");
    auto [ec1, len1] = FlushBuffer();
    RETURN2_IF_FS_ERROR(ec1, len1, "FlushBuffer failed");
    auto [ec2, len2] = _fileNode->Write(buffer, length);
    RETURN2_IF_FS_ERROR(ec2, len2, "Write failed");
    return {FSEC_OK, len1 + len2};
}

FSResult<bool> BufferedFileOutputStream::ForceFlush() noexcept
{
    RETURN2_IF_FS_ERROR(InitFileNode(-1), false, "InitFileNode failed");
    return {FSEC_OK, _fileNode->ForceFlush()};
}

FSResult<void> BufferedFileOutputStream::Close() noexcept
{
    RETURN_IF_FS_ERROR(InitFileNode(-1), "InitFileNode failed");
    RETURN_IF_FS_ERROR(FlushBuffer().Code(), "FlushBuffer failed");
    return _fileNode->Close();
}

size_t BufferedFileOutputStream::GetLength() const noexcept
{
    return _fileNode ? _fileNode->GetLength() : _buffer->GetCursor();
}

ErrorCode BufferedFileOutputStream::InitFileNode(int64_t fileLength) noexcept
{
    if (_fileNode) {
        return FSEC_OK;
    }
    string path = _path;
    _fileNode.reset(CreateFileNode());
    RETURN_IF_FS_ERROR(_fileNode->Open("LOGICAL_PATH", util::PathUtil::NormalizePath(path), FSOT_BUFFERED, fileLength),
                       "");
    return FSEC_OK;
}
BufferedFileNode* BufferedFileOutputStream::CreateFileNode() const noexcept
{
    return new BufferedFileNode(_isAppendMode ? fslib::APPEND : fslib::WRITE);
}

FSResult<size_t> BufferedFileOutputStream::FlushBuffer() noexcept
{
    if (!_buffer) {
        return {FSEC_OK, 0};
    }
    auto ret = _fileNode->Write(_buffer->GetBaseAddr(), _buffer->GetCursor());
    _buffer.reset();
    return ret;
}
}} // namespace indexlib::file_system

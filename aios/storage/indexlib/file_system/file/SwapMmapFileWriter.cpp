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
#include "indexlib/file_system/file/SwapMmapFileWriter.h"

#include <cstddef>
#include <functional>
#include <utility>

#include "alog/Logger.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/Storage.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SwapMmapFileWriter);

SwapMmapFileWriter::SwapMmapFileWriter(const std::shared_ptr<SwapMmapFileNode>& fileNode, Storage* storage,
                                       FileWriterImpl::UpdateFileSizeFunc&& updateFileSizeFunc) noexcept
    : FileWriterImpl(std::move(updateFileSizeFunc))
    , _fileNode(fileNode)
    , _storage(storage)
{
    assert(_fileNode);
    assert(_fileNode->GetOpenType() == FSOT_MMAP);
    assert(_storage);
}

SwapMmapFileWriter::~SwapMmapFileWriter() noexcept {}

ErrorCode SwapMmapFileWriter::DoOpen() noexcept
{
    AUTIL_LOG(ERROR, "not support call open!");
    return FSEC_NOTSUP;
}

FSResult<size_t> SwapMmapFileWriter::Write(const void* buffer, size_t length) noexcept
{
    if (_fileNode) {
        auto [ec, writeLen] = _fileNode->Write(buffer, length);
        RETURN2_IF_FS_ERROR(ec, writeLen, "Write failed");
        _storage->Truncate(_fileNode->GetLogicalPath(), _fileNode->GetLength());
        _updateFileSizeFunc(_fileNode->GetLength());
        return {ec, writeLen};
    }
    return {FSEC_OK, 0};
}

size_t SwapMmapFileWriter::GetLength() const noexcept { return _fileNode ? _fileNode->GetLength() : _length; }

ErrorCode SwapMmapFileWriter::DoClose() noexcept
{
    if (_fileNode) {
        _length = _fileNode->GetLength();
        _fileNode->SetRemainFile();
    }
    _fileNode.reset();
    return FSEC_OK;
}

FSResult<void> SwapMmapFileWriter::Truncate(size_t truncateSize) noexcept
{
    if (_fileNode) {
        _storage->Truncate(_fileNode->GetLogicalPath(), truncateSize);
        _fileNode->Resize(truncateSize);
        _updateFileSizeFunc(truncateSize);
    }
    return FSEC_OK;
}

void SwapMmapFileWriter::TEST_Sync() noexcept(false)
{
    if (_fileNode) {
        _fileNode->Sync().GetOrThrow();
    }
}
}} // namespace indexlib::file_system

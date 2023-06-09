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
#include "indexlib/file_system/file/SliceFileWriter.h"

#include <assert.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/SliceFileNode.h"
#include "indexlib/util/IoExceptionController.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SliceFileWriter);

SliceFileWriter::SliceFileWriter(const std::shared_ptr<SliceFileNode>& sliceFileNode,
                                 UpdateFileSizeFunc&& updateFileSizeFunc) noexcept
    : FileWriterImpl(WriterOption(), std::move(updateFileSizeFunc))
    , _sliceFileNode(sliceFileNode)
    , _length(-1)
{
}

SliceFileWriter::~SliceFileWriter() noexcept
{
    if (_sliceFileNode) {
        AUTIL_LOG(ERROR, "file [%s] not CLOSED in destructor. Exception flag [%d]",
                  _sliceFileNode->DebugString().c_str(), util::IoExceptionController::HasFileIOException());
        assert(util::IoExceptionController::HasFileIOException());
    }
}

ErrorCode SliceFileWriter::DoOpen() noexcept { return FSEC_OK; }

FSResult<size_t> SliceFileWriter::Write(const void* buffer, size_t length) noexcept
{
    assert(_sliceFileNode);
    auto ret = _sliceFileNode->Write(buffer, length);
    // TODO:
    // if (_updateFileSizeFunc)
    // {
    //     _updateFileSizeFunc(GetLength());
    // }
    // TODO: not update size in entry table
    return ret;
}

size_t SliceFileWriter::GetLength() const noexcept { return _sliceFileNode ? _sliceFileNode->GetLength() : _length; }

ErrorCode SliceFileWriter::DoClose() noexcept
{
    if (_sliceFileNode) {
        _length = _sliceFileNode->GetLength();
        _sliceFileNode.reset();
    }
    return FSEC_OK;
}

FSResult<void> SliceFileWriter::ReserveFile(size_t reserveSize) noexcept { return FSEC_OK; }

FSResult<void> SliceFileWriter::Truncate(size_t truncateSize) noexcept
{
    const size_t bufSize = 4 * 1024; // 4k
    char buffer[bufSize] = {0};
    for (size_t i = 0; i < truncateSize / bufSize; i++) {
        RETURN_IF_FS_ERROR(_sliceFileNode->Write(buffer, bufSize).Code(), "write failed");
    }
    uint64_t leftSize = truncateSize % bufSize;
    if (leftSize > 0) {
        RETURN_IF_FS_ERROR(_sliceFileNode->Write(buffer, leftSize).Code(), "write failed");
    }
    if (_updateFileSizeFunc) {
        _updateFileSizeFunc(truncateSize);
    }
    return FSEC_OK;
}

void* SliceFileWriter::GetBaseAddress() noexcept { return _sliceFileNode->GetBaseAddress(); }
}} // namespace indexlib::file_system

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
#include "indexlib/file_system/file/SliceFileReader.h"

#include <cstddef>

#include "alog/Logger.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, SliceFileReader);

SliceFileReader::SliceFileReader(const std::shared_ptr<SliceFileNode>& sliceFileNode) noexcept
    : _sliceFileNode(sliceFileNode)
    , _offset(0)
{
}

SliceFileReader::~SliceFileReader() noexcept
{
    [[maybe_unused]] auto ret = Close();
    assert(ret.OK());
}

FSResult<size_t> SliceFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    return _sliceFileNode->Read(buffer, length, offset, option);
}

FSResult<size_t> SliceFileReader::Read(void* buffer, size_t length, ReadOption option) noexcept
{
    auto [ec, readLen] = _sliceFileNode->Read(buffer, length, _offset, option);
    RETURN2_IF_FS_ERROR(ec, readLen, "Read failed");
    _offset += readLen;
    return {FSEC_OK, readLen};
}

ByteSliceList* SliceFileReader::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    AUTIL_LOG(ERROR, "Not support Read with ByteSliceList");
    return NULL;
}

void* SliceFileReader::GetBaseAddress() const noexcept { return _sliceFileNode->GetBaseAddress(); }

size_t SliceFileReader::GetLength() const noexcept { return _sliceFileNode->GetLength(); }

const string& SliceFileReader::GetLogicalPath() const noexcept { return _sliceFileNode->GetLogicalPath(); }

const std::string& SliceFileReader::GetPhysicalPath() const noexcept { return _sliceFileNode->GetPhysicalPath(); }

FSOpenType SliceFileReader::GetOpenType() const noexcept { return _sliceFileNode->GetOpenType(); }

FSFileType SliceFileReader::GetType() const noexcept { return _sliceFileNode->GetType(); }

FSResult<void> SliceFileReader::Close() noexcept { return FSEC_OK; }
}} // namespace indexlib::file_system

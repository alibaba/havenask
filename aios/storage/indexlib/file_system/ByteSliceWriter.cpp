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
#include "indexlib/file_system/ByteSliceWriter.h"

#include <assert.h>
#include <iosfwd>
#include <stdio.h>
#include <string.h>

#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/IoExceptionController.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, ByteSliceWriter);

ByteSliceWriter::ByteSliceWriter(Pool* pool, uint32_t minSliceSize) noexcept
    : _pool(pool)
    , _lastSliceSize(minSliceSize - ByteSlice::GetHeadSize())
    , _isOwnSliceList(true)
#ifdef PACK_INDEX_MEMORY_STAT
    , _allocatedSize(0)
#endif
{
    _sliceList = AllocateByteSliceList();
    _sliceList->Add(CreateSlice(_lastSliceSize)); // Add the first slice
}

ByteSliceWriter::~ByteSliceWriter() noexcept { Close(); }

ByteSlice* ByteSliceWriter::CreateSlice(uint32_t size) noexcept
{
    ByteSlice* slice = ByteSlice::CreateObject(size, _pool);

#ifdef PACK_INDEX_MEMORY_STAT
    _allocatedSize += size + ByteSlice::GetHeadSize();
#endif

    _lastSliceSize = slice->size;
    slice->size = 0;
    return slice;
}

size_t ByteSliceWriter::GetSize() const noexcept { return size_t(_sliceList->GetTotalSize()); }

FSResult<void> ByteSliceWriter::Dump(const std::shared_ptr<FileWriter>& file) noexcept
{
    assert(_sliceList != NULL);

    ByteSlice* slice = _sliceList->GetHead();
    while (slice != NULL) {
        RETURN_IF_FS_ERROR(file->Write((void*)(slice->data), slice->size).Code(), "Write failed");
        slice = slice->next;
    }
    return FSEC_OK;
}

void ByteSliceWriter::Reset() noexcept
{
    _lastSliceSize = MIN_SLICE_SIZE - ByteSlice::GetHeadSize();
    if (_isOwnSliceList && _sliceList) {
        _sliceList->Clear(_pool);
        DeAllocateByteSliceList(_sliceList);
    }
    _sliceList = AllocateByteSliceList();
    _sliceList->Add(CreateSlice(_lastSliceSize)); // Add the first slice
}

void ByteSliceWriter::Close() noexcept
{
    if (_isOwnSliceList && _sliceList) {
        _sliceList->Clear(_pool);
        DeAllocateByteSliceList(_sliceList);
    }
}

void ByteSliceWriter::Write(const void* value, size_t len) noexcept
{
    uint32_t left = (uint32_t)len;
    uint8_t* data = (uint8_t*)(value);
    ByteSlice* slice = _sliceList->GetTail();
    while (left > 0) {
        if (slice->size >= _lastSliceSize) {
            _lastSliceSize = GetIncrementedSliceSize(_lastSliceSize);
            slice = CreateSlice(_lastSliceSize);
            _sliceList->Add(slice);
        }
        uint32_t nCopyLen = (_lastSliceSize - slice->size) > left ? left : (_lastSliceSize - slice->size);
        memcpy(slice->data + slice->size, data, nCopyLen);
        data += nCopyLen;
        left -= nCopyLen;
        slice->size = slice->size + nCopyLen;
    }
    _sliceList->IncrementTotalSize(len);
}

void ByteSliceWriter::Write(ByteSliceList& src) noexcept { _sliceList->MergeWith(src); }

void ByteSliceWriter::Write(const ByteSliceList& src, uint32_t start, uint32_t end) noexcept(false)
{
    if (start >= end || end > src.GetTotalSize()) {
        char msg[50] = {0};
        snprintf(msg, 50, "start = %u, end = %u, totalSize = %lu", start, end, src.GetTotalSize());
        INDEXLIB_THROW(util::OutOfRangeException, "%s", msg);
    }

    // find start slice
    ByteSlice* currSlice = NULL;
    ByteSlice* nextSlice = src.GetHead();
    uint32_t nextSliceOffset = 0;
    while (start >= nextSliceOffset) {
        currSlice = nextSlice;
        nextSlice = currSlice->next;
        nextSliceOffset += currSlice->size;
    }

    // move and copy data, until reach end slice
    while (end > nextSliceOffset) {
        // copy src[start, nextSliceOffset) to *this
        uint32_t copyLen = nextSliceOffset - start;
        Write(currSlice->data + currSlice->size - copyLen, copyLen);

        // move to next slice, update var start
        currSlice = nextSlice;
        nextSlice = currSlice->next;
        start = nextSliceOffset;
        nextSliceOffset += currSlice->size;
    }
    // copy src[start, end) to *this
    uint32_t copyLen = end - start;
    uint32_t currSliceOffset = nextSliceOffset - currSlice->size;
    Write(currSlice->data + start - currSliceOffset, copyLen);
}

void ByteSliceWriter::SnapShot(ByteSliceWriter& byteSliceWriter) const noexcept
{
    byteSliceWriter.Close();
    byteSliceWriter._sliceList = _sliceList;
    byteSliceWriter._lastSliceSize = _lastSliceSize;
    byteSliceWriter._isOwnSliceList = false;

#ifdef PACK_INDEX_MEMORY_STAT
    byteSliceWriter._allocatedSize = _allocatedSize;
#endif
}
}} // namespace indexlib::file_system

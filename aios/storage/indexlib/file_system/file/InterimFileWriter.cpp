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
#include "indexlib/file_system/file/InterimFileWriter.h"

#include <iosfwd>
#include <string.h>

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, InterimFileWriter);

InterimFileWriter::InterimFileWriter() noexcept : _buf(NULL), _length(0), _cursor(0) {}

InterimFileWriter::~InterimFileWriter() noexcept
{
    if (_buf != NULL) {
        delete[] _buf;
        _length = 0;
        _cursor = 0;
    }
}

void InterimFileWriter::Init(uint64_t length) noexcept
{
    _length = length;
    _cursor = 0;
    if (_buf != NULL) {
        delete[] _buf;
    }
    _buf = new uint8_t[_length];
}

size_t InterimFileWriter::CalculateSize(size_t needLength) noexcept
{
    size_t size = _length;
    while (size < needLength) {
        size *= 2;
    }
    return size;
}

FSResult<size_t> InterimFileWriter::Write(const void* buffer, size_t length) noexcept
{
    assert(buffer != NULL);
    if (length + _cursor > _length) {
        Extend(CalculateSize(length + _cursor));
    }
    memcpy(_buf + _cursor, buffer, length);
    _cursor += length;
    return {FSEC_OK, length};
}

void InterimFileWriter::Extend(size_t extendSize) noexcept
{
    assert(extendSize > _length);
    uint8_t* dstBuffer = new uint8_t[extendSize];
    memcpy(dstBuffer, _buf, _cursor);
    uint8_t* tmpBuffer = _buf;
    _buf = dstBuffer;
    _length = extendSize;
    delete[] tmpBuffer;
}

ByteSliceListPtr InterimFileWriter::GetByteSliceList(bool isVIntHeader) const noexcept
{
    ByteSlice* slice = ByteSlice::CreateObject(0);
    if (isVIntHeader) {
        char* cursor = (char*)_buf;
        uint32_t len = ReadVUInt32(cursor);
        (void)len;
        slice->size = _length - (cursor - (char*)_buf);
        slice->data = (uint8_t*)cursor;
    } else {
        slice->size = _length - sizeof(uint32_t);
        slice->dataSize = slice->size;
        slice->data = _buf + sizeof(uint32_t);
    }
    ByteSliceList* sliceList = new ByteSliceList(slice);
    return ByteSliceListPtr(sliceList);
}

ByteSliceListPtr InterimFileWriter::CopyToByteSliceList(bool isVIntHeader) noexcept
{
    ByteSlice* slice = NULL;
    if (isVIntHeader) {
        char* cursor = (char*)_buf;
        uint32_t len = ReadVUInt32(cursor);
        (void)len;
        uint32_t headerLen = cursor - (char*)_buf;
        slice = ByteSlice::CreateObject(_length - headerLen);
        memcpy(slice->data, cursor, _length - headerLen);
    } else {
        slice = ByteSlice::CreateObject(_length - sizeof(uint32_t));
        memcpy(slice->data, _buf + sizeof(uint32_t), _length - sizeof(uint32_t));
    }

    _length = 0;
    _cursor = 0;
    ByteSliceList* sliceList = new ByteSliceList(slice);
    return ByteSliceListPtr(sliceList);
}
}} // namespace indexlib::file_system

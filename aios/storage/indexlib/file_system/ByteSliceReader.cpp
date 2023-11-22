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
#include "indexlib/file_system/ByteSliceReader.h"

#include <ostream>
#include <stdint.h>
#include <string.h>
#include <string>

#include "indexlib/util/DeVirtual.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/IoExceptionController.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

////////////////////////////////////////////////////
AUTIL_LOG_SETUP(indexlib.file_system, ByteSliceReader);

ByteSliceReader::ByteSliceReader()
    : _currentSlice(NULL)
    , _currentSliceOffset(0)
    , _globalOffset(0)
    , _sliceList(nullptr)
    , _size(0)
    , _isBlockByteSliceList(false)
{
}

FSResult<void> ByteSliceReader::Open(ByteSliceList* sliceList)
{
    Close();
    _sliceList = sliceList;
    _size = GetSize();

    auto headSlice = _sliceList->GetHead();
    if (sliceList->getObjectType() == IE_SUB_CLASS_TYPE_NAME(ByteSliceList, BlockByteSliceList)) {
        _isBlockByteSliceList = true;
        auto ret = PrepareSlice(headSlice);
        RETURN_IF_FS_ERROR(ret.Code(), "PrepareSlice failed");
        headSlice = ret.Value();
    }

    _currentSlice = headSlice;
    _globalOffset = 0;
    _currentSliceOffset = 0;

    if (_currentSlice == NULL) {
        return {FSEC_ERROR};
    }
    return {FSEC_OK};
}

FSResult<void> ByteSliceReader::Open(ByteSlice* slice)
{
    Close();
    _currentSlice = slice;
    _globalOffset = 0;
    _currentSliceOffset = 0;

    if (_currentSlice == NULL) {
        return {FSEC_ERROR};
    }
    return {FSEC_OK};
}

void ByteSliceReader::Close()
{
    _currentSlice = NULL;
    _globalOffset = 0;
    _currentSliceOffset = 0;
    _size = 0;
    _isBlockByteSliceList = false;
    if (_sliceList) {
        _sliceList = NULL;
    }
}

FSResult<size_t> ByteSliceReader::Read(void* value, size_t len)
{
    if (_currentSlice == NULL || len == 0) {
        return {FSEC_OK, 0};
    }

    if (_currentSliceOffset + len <= GetSliceDataSize(_currentSlice)) {
        memcpy(value, _currentSlice->data + _currentSliceOffset, len);
        _currentSliceOffset += len;
        _globalOffset += len;
        return {FSEC_OK, len};
    }
    // current byteslice is not long enough, read next byteslices
    char* dest = (char*)value;
    int64_t totalLen = (int64_t)len;
    size_t offset = _currentSliceOffset;
    int64_t leftLen = 0;
    while (totalLen > 0) {
        leftLen = GetSliceDataSize(_currentSlice) - offset;
        if (leftLen < totalLen) {
            memcpy(dest, _currentSlice->data + offset, leftLen);
            totalLen -= leftLen;
            dest += leftLen;

            offset = 0;
            auto ret = NextSlice(_currentSlice);
            RETURN2_IF_FS_ERROR(ret.Code(), 0, "NextSlice failed");
            _currentSlice = ret.Value();
            if (!_currentSlice) {
                break;
            }
        } else {
            memcpy(dest, _currentSlice->data + offset, totalLen);
            dest += totalLen;
            offset += (size_t)totalLen;
            totalLen = 0;
        }
    }

    _currentSliceOffset = offset;
    size_t readLen = (size_t)(len - totalLen);
    _globalOffset += readLen;
    return {FSEC_OK, readLen};
}

FSResult<size_t> ByteSliceReader::ReadMayCopy(void*& value, size_t len)
{
    if (_currentSlice == NULL || len == 0) {
        return {FSEC_OK, 0};
    }

    if (_currentSliceOffset + len <= GetSliceDataSize(_currentSlice)) {
        value = _currentSlice->data + _currentSliceOffset;
        _currentSliceOffset += len;
        _globalOffset += len;
        return {FSEC_OK, len};
    }
    return Read(value, len);
}

FSResult<size_t> ByteSliceReader::Seek(size_t offset)
{
    if (offset < _globalOffset) {
        RETURN2_IF_FS_ERROR(FSEC_ERROR, 0, "Invalid offset value: seek offset = %zu", offset);
    }

    size_t len = offset - _globalOffset;
    if (_currentSlice == NULL || len == 0) {
        return {FSEC_OK, _globalOffset};
    }

    if (_currentSliceOffset + len < GetSliceDataSize(_currentSlice)) {
        _currentSliceOffset += len;
        _globalOffset += len;
        return {FSEC_OK, _globalOffset};
    } else {
        // current byteslice is not long enough, seek to next byteslices
        int64_t totalLen = len;
        int64_t remainLen;
        while (totalLen > 0) {
            remainLen = _currentSlice->size - _currentSliceOffset;
            if (remainLen <= totalLen) {
                _globalOffset += remainLen;
                totalLen -= remainLen;
                _currentSlice = _currentSlice->next;
                _currentSliceOffset = 0;
                if (_currentSlice == NULL) {
                    if (_globalOffset == GetSize()) {
                        _globalOffset = BYTE_SLICE_EOF;
                    }
                    return {FSEC_OK, size_t(BYTE_SLICE_EOF)};
                }
            } else {
                _globalOffset += totalLen;
                _currentSliceOffset += totalLen;
                totalLen = 0;
            }
        }
        if (_globalOffset == GetSize()) {
            _globalOffset = BYTE_SLICE_EOF;
            _currentSlice = ByteSlice::GetImmutableEmptyObject();
            _currentSliceOffset = 0;
        } else {
            auto ret = PrepareSlice(_currentSlice);
            RETURN2_IF_FS_ERROR(ret.Code(), 0, "PrepareSlice failed");
            _currentSlice = ret.Value();
        }
        return {FSEC_OK, _globalOffset};
    }
}

}} // namespace indexlib::file_system

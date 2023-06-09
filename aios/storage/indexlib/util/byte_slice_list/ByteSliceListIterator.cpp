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
#include "indexlib/util/byte_slice_list/ByteSliceListIterator.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, ByteSliceListIterator);

ByteSliceListIterator::ByteSliceListIterator(const ByteSliceList* sliceList)
    : _sliceList(sliceList)
    , _slice(sliceList->GetHead())
    , _posInSlice(0)
    , _seekedSliceSize(0)
    , _endPos(0)
{
}

ByteSliceListIterator::ByteSliceListIterator(const ByteSliceListIterator& other)
    : _sliceList(other._sliceList)
    , _slice(other._slice)
    , _posInSlice(other._posInSlice)
    , _seekedSliceSize(other._seekedSliceSize)
    , _endPos(other._endPos)
{
}

bool ByteSliceListIterator::SeekSlice(size_t beginPos)
{
    if (beginPos < _seekedSliceSize + _posInSlice) {
        return false;
    }

    while (_slice) {
        size_t sliceEndPos = _seekedSliceSize + _slice->size;
        if (beginPos >= _seekedSliceSize && beginPos < sliceEndPos) {
            _posInSlice = beginPos - _seekedSliceSize;
            return true;
        }

        _seekedSliceSize += _slice->size;
        _posInSlice = 0;
        _slice = _slice->next;
    }
    return false;
}

bool ByteSliceListIterator::HasNext(size_t endPos)
{
    if (_slice == NULL || endPos > _sliceList->GetTotalSize()) {
        return false;
    }

    _endPos = endPos;
    size_t curPos = _seekedSliceSize + _posInSlice;
    return curPos < endPos;
}

void ByteSliceListIterator::Next(void*& data, size_t& size)
{
    assert(_slice != NULL);

    size_t curPos = _seekedSliceSize + _posInSlice;
    size_t sliceEndPos = _seekedSliceSize + _slice->size;

    data = _slice->data + _posInSlice;
    if (_endPos >= sliceEndPos) {
        size = sliceEndPos - curPos;
        _seekedSliceSize += _slice->size;
        _posInSlice = 0;
        _slice = _slice->next;
    } else {
        size = _endPos - curPos;
        _posInSlice = _endPos - _seekedSliceSize;
    }
}
}} // namespace indexlib::util

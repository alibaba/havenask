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
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

#include "indexlib/util/Exception.h"
using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::util;
namespace indexlib { namespace util {

////////////////////////////////////////////////////
AUTIL_LOG_SETUP(indexlib.util, ByteSliceList);

ByteSliceList::ByteSliceList() noexcept
{
    _head = NULL;
    _tail = NULL;
    _totalSize = 0;
}

ByteSliceList::ByteSliceList(ByteSlice* slice) noexcept : _head(NULL), _tail(NULL), _totalSize(0)
{
    if (slice != NULL) {
        Add(slice);
    }
}

ByteSliceList::~ByteSliceList() noexcept
{
    // we should call Clear() explicitly when using pool
    Clear(NULL);
    assert(!_head);
    assert(!_tail);
}

void ByteSliceList::Add(ByteSlice* slice) noexcept
{
    if (_tail == NULL) {
        _head = _tail = slice;
    } else {
        _tail->next = slice;
        _tail = slice;
    }
    _totalSize = _totalSize + slice->size;
}

void ByteSliceList::Clear(Pool* pool) noexcept
{
    ByteSlice* slice = _head;
    ByteSlice* next = NULL;

    while (slice) {
        next = slice->next;
        ByteSlice::DestroyObject(slice, pool);
        slice = next;
    }

    _head = _tail = NULL;
    _totalSize = 0;
}

size_t ByteSliceList::UpdateTotalSize() noexcept
{
    _totalSize = 0;
    ByteSlice* slice = _head;
    while (slice) {
        _totalSize = _totalSize + slice->size;
        slice = slice->next;
    }
    return _totalSize;
}

void ByteSliceList::MergeWith(ByteSliceList& other) noexcept
{
    // assert(this->_pool == other._pool);

    if (_head == NULL) {
        _head = other._head;
        _tail = other._tail;
    } else {
        _tail->next = other._head;
        _tail = other._tail;
    }

    _totalSize = _totalSize + other._totalSize;
    other._head = other._tail = NULL;
    other._totalSize = 0;
}
}} // namespace indexlib::util

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
#include "indexlib/index/common/data_structure/VarLenDataIterator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenDataIterator);

bool VarLenDataIterator::HasNext()
{
    if (_cursor == INVALID_CURSOR) {
        return _offsets->Size() > 0;
    }

    return _cursor < _offsets->Size() - 1;
}

void VarLenDataIterator::Next()
{
    if (_cursor == INVALID_CURSOR) {
        _cursor = 0;
    } else {
        _cursor++;
    }

    if (_cursor >= _offsets->Size()) {
        return;
    }

    _currentOffset = (*_offsets)[_cursor];
    uint8_t* buffer = _data->Search(_currentOffset);
    _dataLength = *(uint32_t*)buffer;
    _currentData = buffer + sizeof(uint32_t);
}

void VarLenDataIterator::GetCurrentData(uint64_t& dataLength, uint8_t*& data)
{
    if (_cursor == INVALID_CURSOR || _cursor == _offsets->Size()) {
        AUTIL_LOG(ERROR, "current cursor is invalid. [%lu]", _cursor);
        data = NULL;
        dataLength = 0;
        return;
    }
    dataLength = _dataLength;
    data = _currentData;
}

void VarLenDataIterator::GetCurrentOffset(uint64_t& offset)
{
    if (_cursor == INVALID_CURSOR || _cursor == _offsets->Size()) {
        AUTIL_LOG(ERROR, "current cursor is invalid. [%lu]", _cursor);
        offset = 0;
        return;
    }
    offset = _currentOffset;
}
} // namespace indexlibv2::index

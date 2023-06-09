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
#include "indexlib/index/kv/VarLenKVSegmentIterator.h"

#include "indexlib/index/kv/KVKeyIterator.h"
#include "indexlib/index/kv/ValueReader.h"

namespace indexlibv2::index {

VarLenKVSegmentIterator::VarLenKVSegmentIterator(bool shortOffset, std::unique_ptr<KVKeyIterator> keyIterator,
                                                 std::unique_ptr<ValueReader> valueReader)
    : _shortOffset(shortOffset)
    , _keyIterator(std::move(keyIterator))
    , _valueReader(std::move(valueReader))
{
}

VarLenKVSegmentIterator::~VarLenKVSegmentIterator() {}

bool VarLenKVSegmentIterator::HasNext() const { return _keyIterator->HasNext(); }

Status VarLenKVSegmentIterator::Next(autil::mem_pool::Pool* pool, Record& record)
{
    auto s = _keyIterator->Next(pool, record);
    if (!s.IsOK()) {
        return s;
    }
    if (record.deleted) {
        return s;
    }
    offset_t offset = 0;
    if (_shortOffset) {
        offset = *(short_offset_t*)record.value.data();
    } else {
        offset = *(offset_t*)record.value.data();
    }
    return _valueReader->Read(offset, pool, record.value);
}

Status VarLenKVSegmentIterator::Seek(offset_t offset) { return _keyIterator->Seek(offset); }

void VarLenKVSegmentIterator::Reset() { _keyIterator->Reset(); }

offset_t VarLenKVSegmentIterator::GetOffset() const { return _keyIterator->GetOffset(); }

const IKVIterator* VarLenKVSegmentIterator::GetKeyIterator() const { return _keyIterator.get(); }

} // namespace indexlibv2::index

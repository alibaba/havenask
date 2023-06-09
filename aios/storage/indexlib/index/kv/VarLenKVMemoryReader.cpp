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
#include "indexlib/index/kv/VarLenKVMemoryReader.h"

#include "indexlib/index/kv/IKVIterator.h"

namespace indexlibv2::index {

VarLenKVMemoryReader::VarLenKVMemoryReader() {}

VarLenKVMemoryReader::~VarLenKVMemoryReader()
{
    _hashTable.reset();
    _valueAccessor.reset();
}

void VarLenKVMemoryReader::SetKey(const std::shared_ptr<HashTableBase>& hashTable,
                                  const std::shared_ptr<ValueUnpacker>& valueUnpacker, bool isShortOffset)
{
    _hashTable = hashTable;
    _valueUnpacker = valueUnpacker;
    _isShortOffset = isShortOffset;
}

void VarLenKVMemoryReader::SetValue(
    const std::shared_ptr<indexlibv2::index::ExpandableValueAccessor<uint64_t>>& accessor, int32_t valueFixedLen)
{
    _valueAccessor = accessor;
    _valueFixedLen = valueFixedLen;
}

void VarLenKVMemoryReader::HoldDataPool(const std::shared_ptr<autil::mem_pool::PoolBase>& pool) { _dataPool = pool; }

void VarLenKVMemoryReader::SetPlainFormatEncoder(const std::shared_ptr<PlainFormatEncoder>& encoder)
{
    _plainFormatEncoder = encoder;
}

std::unique_ptr<IKVIterator> VarLenKVMemoryReader::CreateIterator() { return nullptr; }

} // namespace indexlibv2::index

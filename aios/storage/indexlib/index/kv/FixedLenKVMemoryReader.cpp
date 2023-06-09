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
#include "indexlib/index/kv/FixedLenKVMemoryReader.h"

#include "indexlib/index/kv/IKVIterator.h"

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, FixedLenKVMemoryReader);

FixedLenKVMemoryReader::FixedLenKVMemoryReader() {}

FixedLenKVMemoryReader::~FixedLenKVMemoryReader() {}

void FixedLenKVMemoryReader::SetKey(const std::shared_ptr<HashTableBase>& hashTable,
                                    const std::shared_ptr<ValueUnpacker>& valueUnpacker, KVTypeId typeId)
{
    _hashTable = hashTable;
    _valueUnpacker = valueUnpacker;
    _typeId = typeId;
}

void FixedLenKVMemoryReader::HoldDataPool(const std::shared_ptr<autil::mem_pool::PoolBase>& pool) { _dataPool = pool; }

std::unique_ptr<IKVIterator> FixedLenKVMemoryReader::CreateIterator() { return nullptr; }

} // namespace indexlibv2::index

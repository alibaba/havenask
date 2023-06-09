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
#include "indexlib/index/kv/KVKeyIterator.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"

namespace indexlibv2::index {

KVKeyIterator::KVKeyIterator(std::unique_ptr<HashTableFileIterator> hashIter, ValueUnpacker* valueUnpacker,
                             bool isVarLen)
    : _hashIter(std::move(hashIter))
    , _valueUnpacker(valueUnpacker)
    , _isVarLen(isVarLen)
{
}

KVKeyIterator::~KVKeyIterator() {}

bool KVKeyIterator::HasNext() const { return _hashIter->IsValid(); }

Status KVKeyIterator::Next(autil::mem_pool::Pool* pool, Record& record)
{
    record.key = _hashIter->GetKey();
    record.deleted = _hashIter->IsDeleted();

    auto value = _hashIter->GetValue();
    _valueUnpacker->Unpack(value, record.timestamp, record.value);
    // closed buffer file iterator in fixed len case, we sholud copy buffer because MoveToNext will cover the value
    // TODO(xinfei.sxf) optimize this to reduce the copy cost
    if (!_isVarLen) {
        size_t len = record.value.size();
        auto buffer = pool->allocate(len);
        memcpy(buffer, record.value.data(), len);
        record.value = autil::StringView((char*)buffer, len);
    }
    try {
        _hashIter->MoveToNext();
    } catch (const std::exception& e) {
        return Status::IOError(e.what());
    }
    return Status::OK();
}

Status KVKeyIterator::Seek(offset_t offset)
{
    try {
        if (!_hashIter->Seek(offset)) {
            return Status::OutOfRange();
        }
    } catch (const std::exception& e) {
        return Status::IOError(e.what());
    }
    return Status::OK();
}

void KVKeyIterator::Reset() { _hashIter->Reset(); }

offset_t KVKeyIterator::GetOffset() const { return _hashIter->GetOffset(); }

const IKVIterator* KVKeyIterator::GetKeyIterator() const { return nullptr; }

void KVKeyIterator::SortByKey() { _hashIter->SortByKey(); }

void KVKeyIterator::SortByValue() { _hashIter->SortByValue(); }

} // namespace indexlibv2::index

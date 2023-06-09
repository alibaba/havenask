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
#include "indexlib/index/kv/FixedLenKVSegmentIterator.h"

#include "indexlib/index/kv/FixedLenValueExtractorUtil.h"
#include "indexlib/index/kv/KVKeyIterator.h"
#include "indexlib/index/kv/ValueReader.h"

namespace indexlibv2::index {

AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, FixedLenKVSegmentIterator);

FixedLenKVSegmentIterator::FixedLenKVSegmentIterator(const KVTypeId& typeId, std::unique_ptr<KVKeyIterator> keyIterator)
    : _typeId(typeId)
    , _keyIterator(std::move(keyIterator))
{
}

FixedLenKVSegmentIterator::~FixedLenKVSegmentIterator() {}

bool FixedLenKVSegmentIterator::HasNext() const { return _keyIterator->HasNext(); }

Status FixedLenKVSegmentIterator::Next(autil::mem_pool::Pool* pool, Record& record)
{
    auto status = _keyIterator->Next(pool, record);
    if (!status.IsOK()) {
        return status;
    }
    if (record.deleted) {
        return status;
    }
    auto ret = FixedLenValueExtractorUtil::ValueExtract((void*)record.value.data(), _typeId, pool, record.value);
    if (!ret) {
        AUTIL_LOG(ERROR, "value extract failed, typeId[%s], value len = [%lu]", _typeId.ToString().c_str(),
                  record.value.size());
        return Status::Corruption();
    } else {
        return Status::OK();
    }
}

Status FixedLenKVSegmentIterator::Seek(offset_t offset) { return _keyIterator->Seek(offset); }

void FixedLenKVSegmentIterator::Reset() { _keyIterator->Reset(); }

offset_t FixedLenKVSegmentIterator::GetOffset() const { return _keyIterator->GetOffset(); }

const IKVIterator* FixedLenKVSegmentIterator::GetKeyIterator() const { return _keyIterator.get(); }

} // namespace indexlibv2::index

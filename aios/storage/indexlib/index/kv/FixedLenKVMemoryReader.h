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
#pragma once

#include <memory>

#include "autil/MultiValueType.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/kv/FixedLenValueExtractorUtil.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVTypeId.h"

namespace indexlibv2::index {

class FixedLenKVMemoryReader final : public IKVSegmentReader
{
public:
    FixedLenKVMemoryReader();
    ~FixedLenKVMemoryReader();

public:
    void SetKey(const std::shared_ptr<HashTableBase>& hashTable, const std::shared_ptr<ValueUnpacker>& valueUnpacker,
                KVTypeId typeId);
    void HoldDataPool(const std::shared_ptr<autil::mem_pool::PoolBase>& pool);

public:
    FL_LAZY(indexlib::util::Status)
    Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool = nullptr,
        KVMetricsCollector* collector = nullptr, autil::TimeoutTerminator* timeoutTerminator = nullptr) const override;
    std::unique_ptr<IKVIterator> CreateIterator() override;
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    KVTypeId _typeId;
    std::shared_ptr<HashTableBase> _hashTable;
    std::shared_ptr<ValueUnpacker> _valueUnpacker;
    std::shared_ptr<autil::mem_pool::PoolBase> _dataPool; // hold

private:
    AUTIL_LOG_DECLARE();
};

inline FL_LAZY(indexlib::util::Status) FixedLenKVMemoryReader::Get(keytype_t key, autil::StringView& value,
                                                                   uint64_t& ts, autil::mem_pool::Pool* pool,
                                                                   KVMetricsCollector* collector,
                                                                   autil::TimeoutTerminator* timeoutTerminator) const
{
    autil::StringView str;
    auto ret = _hashTable->Find(key, str);
    if (ret != indexlib::util::OK && ret != indexlib::util::DELETED) {
        FL_CORETURN ret;
    }
    autil::StringView packedData;
    _valueUnpacker->Unpack(str, ts, packedData);
    if (ret == indexlib::util::DELETED) {
        FL_CORETURN ret;
    }
    auto status = FixedLenValueExtractorUtil::ValueExtract((void*)packedData.data(), _typeId, pool, value);
    if (!status) {
        AUTIL_LOG(ERROR, "value extract failed, typeId[%s], value len = [%lu]", _typeId.ToString().c_str(),
                  packedData.size());
    }
    ret = status ? indexlib::util::OK : indexlib::util::FAIL;
    FL_CORETURN ret;
}

} // namespace indexlibv2::index

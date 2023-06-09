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
#include "indexlib/index/common/data_structure/ExpandableValueAccessor.h"
#include "indexlib/index/common/field_format/pack_attribute/PlainFormatEncoder.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/kv/IKVSegmentReader.h"

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlib::util {
template <typename T>
class ExpandableValueAccessor;
}

namespace indexlibv2::index {

class VarLenKVMemoryReader final : public IKVSegmentReader
{
public:
    VarLenKVMemoryReader();
    ~VarLenKVMemoryReader();

public:
    void SetKey(const std::shared_ptr<HashTableBase>& hashTable, const std::shared_ptr<ValueUnpacker>& valueUnpacker,
                bool shortOffset);
    void SetValue(const std::shared_ptr<indexlibv2::index::ExpandableValueAccessor<uint64_t>>& accessor,
                  int32_t valueFixedLen);
    void HoldDataPool(const std::shared_ptr<autil::mem_pool::PoolBase>& pool);
    void SetPlainFormatEncoder(const std::shared_ptr<PlainFormatEncoder>& encoder);

public:
    FL_LAZY(indexlib::util::Status)
    Get(keytype_t key, autil::StringView& value, uint64_t& ts, autil::mem_pool::Pool* pool = nullptr,
        KVMetricsCollector* collector = nullptr, autil::TimeoutTerminator* timeoutTerminator = nullptr) const override;
    std::unique_ptr<IKVIterator> CreateIterator() override;
    size_t EvaluateCurrentMemUsed() override { return 0; }

private:
    std::shared_ptr<autil::mem_pool::PoolBase> _dataPool; // hold
    std::shared_ptr<HashTableBase> _hashTable;
    std::shared_ptr<ValueUnpacker> _valueUnpacker;
    std::shared_ptr<PlainFormatEncoder> _plainFormatEncoder;
    std::shared_ptr<indexlibv2::index::ExpandableValueAccessor<uint64_t>> _valueAccessor;
    int32_t _valueFixedLen = -1;
    bool _isShortOffset = false;
};

inline FL_LAZY(indexlib::util::Status) VarLenKVMemoryReader::Get(keytype_t key, autil::StringView& value, uint64_t& ts,
                                                                 autil::mem_pool::Pool* pool,
                                                                 KVMetricsCollector* collector,
                                                                 autil::TimeoutTerminator* timeoutTerminator) const
{
    autil::StringView str;
    auto ret = _hashTable->FindForReadWrite(key, str, pool);
    if (ret != indexlib::util::OK && ret != indexlib::util::DELETED) {
        FL_CORETURN ret;
    }
    autil::StringView packedData;
    _valueUnpacker->Unpack(str, ts, packedData);
    if (ret == indexlib::util::DELETED) {
        FL_CORETURN ret;
    }
    offset_t offset = 0;
    if (_isShortOffset) {
        offset = *reinterpret_cast<const short_offset_t*>(packedData.data());
    } else {
        offset = *reinterpret_cast<const offset_t*>(packedData.data());
    }
    if (_valueFixedLen > 0) {
        assert(!_plainFormatEncoder);
        autil::MultiChar mc(_valueAccessor->GetValue(offset), _valueFixedLen);
        value = {mc.data(), mc.size()};
    } else {
        autil::MultiChar mc(_valueAccessor->GetValue(offset));
        value = {mc.data(), mc.size()};
        if (_plainFormatEncoder) {
            auto status = _plainFormatEncoder->Decode(value, pool, value);
            FL_CORETURN status ? indexlib::util::OK : indexlib::util::FAIL;
        }
    }
    FL_CORETURN indexlib::util::OK;
}

} // namespace indexlibv2::index

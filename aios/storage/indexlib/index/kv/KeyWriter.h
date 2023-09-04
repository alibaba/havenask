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
#include <type_traits>

#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/Record.h"
#include "indexlib/index/kv/VarLenHashTableCollector.h"
#include "indexlib/util/Status.h"

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {
class BucketCompressor;
class HashTableBase;
class ValueUnpacker;
struct HashTableOptions;
struct MemoryUsage;
struct SegmentStatistics;

class KeyWriter
{
public:
    typedef VarLenHashTableCollector::CollectFuncType CollectFuncType;

public:
    KeyWriter();
    virtual ~KeyWriter();

public:
    Status Init(const KVTypeId& typeId);
    Status AllocateMemory(autil::mem_pool::PoolBase* pool, size_t maxMemoryUse, int32_t occupancyPct);

    bool KeyExist(uint64_t key) const;
    bool IsFull() { return _hashTable->IsFull(); }
    bool Find(uint64_t key, autil::StringView& value, uint32_t& timestamp);
    virtual Status Add(uint64_t key, const autil::StringView& value, uint32_t timestamp);
    virtual Status Delete(uint64_t key, uint32_t timestamp);
    Status Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory);
    Status Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory, bool shrink, bool compress);

public:
    void FillStatistics(SegmentStatistics& stat) const;
    void FillMemoryUsage(MemoryUsage& usage) const;

public:
    template <typename ValueType>
    Status AddSimple(uint64_t key, ValueType value, uint32_t timestamp)
    {
        static_assert(std::is_arithmetic<ValueType>::value, "unexpected type");
        autil::StringView str(reinterpret_cast<char*>(&value), sizeof(value));
        return Add(key, str, timestamp);
    }

    void CollectRecord(const CollectFuncType& func);

    const std::shared_ptr<HashTableBase>& GetHashTable() const { return _hashTable; }
    const std::shared_ptr<ValueUnpacker>& GetValueUnpacker() const { return _valuePacker; }

private:
    virtual void FillHashTableOptions(HashTableOptions& opts) const;

protected:
    KVTypeId _typeId;
    std::shared_ptr<HashTableBase> _hashTable;
    std::shared_ptr<ValueUnpacker> _valuePacker;
    std::unique_ptr<BucketCompressor> _bucketCompressor;
};

} // namespace indexlibv2::index

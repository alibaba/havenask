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
#ifndef __INDEXLIB_HASH_TABLE_BASE_H
#define __INDEXLIB_HASH_TABLE_BASE_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common/hash_table/bucket_compressor.h"
#include "indexlib/common/hash_table/hash_table_options.h"
#include "indexlib/common/hash_table/special_value.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status.h"

namespace indexlib { namespace common {

enum HashTableAccessType {
    DENSE_TABLE,
    CUCKOO_TABLE,
    DENSE_READER,
    CUCKOO_READER,
};

typedef std::map<std::string, std::string> KVMap;

class HashTableFileIterator
{
public:
    virtual ~HashTableFileIterator() {}
    virtual bool Init(const file_system::FileReaderPtr& fileReader) = 0;
    virtual bool IsValid() const = 0;
    virtual size_t Size() const = 0;
    virtual void MoveToNext() = 0;
    virtual uint64_t GetKey() const = 0;
    virtual autil::StringView GetValue() const = 0;
    virtual bool IsDeleted() const = 0;
    virtual size_t EstimateMemoryUse(size_t keyCount) const = 0;
    virtual void SortByKey() = 0;
    virtual void SortByValue() = 0;
    virtual bool Seek(int64_t offset) = 0;
    virtual void Reset() = 0;
    virtual int64_t GetOffset() const = 0;
};

class HashTableAccessor
{
public:
    virtual ~HashTableAccessor() {}
};

class HashTableBase : public HashTableAccessor
{
public:
    HashTableBase();
    virtual ~HashTableBase();

public:
    virtual util::Status Find(uint64_t key, autil::StringView& value) const = 0;
    virtual util::Status FindForReadWrite(uint64_t key, autil::StringView& value,
                                          autil::mem_pool::Pool* pool) const = 0;
    virtual bool MountForRead(const void* data, size_t size) = 0;
    virtual bool Insert(uint64_t key, const autil::StringView& value) = 0;
    virtual bool Delete(uint64_t key, const autil::StringView& value = autil::StringView()) = 0;
    virtual bool IsFull() const = 0;
    virtual uint64_t BuildAssistantMemoryUse() const = 0;
    virtual bool MountForWrite(void* data, size_t size, const HashTableOptions& options = INVALID_OCCUPANCY) = 0;
    virtual bool Shrink(int32_t occupancyPct = 0) = 0;
    virtual uint64_t MemoryUse() const = 0;
    virtual uint64_t Size() const = 0;
    virtual int32_t GetOccupancyPct() const = 0;
    virtual uint64_t GetDeleteCount() const = 0;
    virtual uint64_t Capacity() const = 0;
    virtual void* Address() const = 0;
    virtual bool Stretch() = 0;
    virtual size_t Compress(BucketCompressor* bucketCompressor) = 0;

public:
    virtual int32_t GetRecommendedOccupancy(int32_t occupancy) const = 0;
    virtual uint64_t CapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options) const = 0;
    virtual uint64_t CapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& optionos) const = 0;
    virtual size_t TableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct) const = 0;
    virtual size_t BuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct) const = 0;
    virtual size_t BuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct) const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(HashTableBase);

struct HashTableInfo {
    std::unique_ptr<HashTableAccessor> hashTable;
    std::unique_ptr<common::ValueUnpacker> valueUnpacker;
    std::unique_ptr<BucketCompressor> bucketCompressor;
    std::unique_ptr<HashTableFileIterator> hashTableFileIterator;

    template <typename From, typename To>
    inline To* Cast(From* f)
    {
        static_assert(std::is_base_of<From, To>::value, "cast failed");
        return dynamic_cast<To*>(f);
    }

    template <typename T>
    T* GetHashTable()
    {
        return Cast<HashTableAccessor, T>(hashTable.get());
    }

    template <typename T>
    std::unique_ptr<T> StealHashTable()
    {
        return std::unique_ptr<T>(Cast<HashTableAccessor, T>(hashTable.release()));
    }
};
}} // namespace indexlib::common

#endif //__INDEXLIB_HASH_TABLE_BASE_H

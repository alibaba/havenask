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

#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"

namespace indexlib { namespace common {

template <typename _KT, typename _VT, bool hasDeleted>
struct ClosedHashTableKVTuple;

template <typename _KT, typename _VT>
struct ClosedHashTableKVTuple<_KT, _VT, true> {
    _KT key;
    _VT value;
    bool deleted;
    ClosedHashTableKVTuple(const _KT& _key, const _VT& _value, bool _deleted)
        : key(_key)
        , value(_value)
        , deleted(_deleted)
    {
    }
    const _KT& Key() const { return key; }
    const _VT& Value() const { return value; }
    const void* ValueAddr() const { return std::addressof(value); }
    bool IsDeleted() const { return deleted; }
};

template <typename _KT, typename _VT>
struct ClosedHashTableKVTuple<_KT, _VT, false> {
    _KT key;
    _VT value;
    ClosedHashTableKVTuple(const _KT& _key, const _VT& _value, bool _deleted) : key(_key), value(_value) {}
    const _KT& Key() const { return key; }
    const _VT& Value() const { return value; }
    const void* ValueAddr() const { return std::addressof(value); }
    bool IsDeleted() const { return false; }
};

// FORMAT: see dense_hash_table.h or cuckoo_hash_table.h
template <typename HashTableHeader, typename _KT, typename _VT,
          bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey, bool hasDeleted = true,
          bool useCompactBucket = false>
class ClosedHashTableFileIterator : public HashTableFileIterator
{
public:
    ClosedHashTableFileIterator() : mCursor(0) {}
    ~ClosedHashTableFileIterator() = default;

protected:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    typedef ClosedHashTableKVTuple<_KT, _VT, hasDeleted> KVTuple;
    typedef std::vector<KVTuple> KVTupleVec;

public:
    size_t EstimateMemoryUse(size_t keyCount) const override final { return sizeof(KVTuple) * keyCount; }

public:
    bool Init(const file_system::FileReaderPtr& fileReader) override
    {
        HashTableHeader header;
        auto [status, readedSize] = fileReader->Read(&header, sizeof(header), 0).StatusWith();
        if (!status.IsOK() || readedSize != sizeof(header)) {
            IE_LOG(ERROR, "read header failed, status[%s], header size[%lu], fileLength[%lu]",
                   status.ToString().c_str(), sizeof(header), fileReader->GetLogicLength());
            return false;
        }
        IE_LOG(INFO, "iterator reserve size [%lu]", header.keyCount * sizeof(KVTuple));
        mKVTupleVec.reserve(header.keyCount);
        for (size_t i = 0; i < header.bucketCount; ++i) {
            Bucket bucket;
            std::tie(status, readedSize) = fileReader->Read(&bucket, sizeof(bucket)).StatusWith();
            if (!status.IsOK() || readedSize != sizeof(bucket)) {
                IE_LOG(ERROR, "read bucket[%lu] failed, status[%s]", i, status.ToString().c_str());
                return false;
            }
            if (!bucket.IsEmpty()) {
                KVTuple tuple(bucket.Key(), bucket.Value(), bucket.IsDeleted());
                mKVTupleVec.push_back(tuple);
            }
        }
        return true;
    }

    size_t Size() const override final { return mKVTupleVec.size(); }
    bool IsValid() const override final { return mCursor < (int64_t)mKVTupleVec.size(); }
    void MoveToNext() override final { ++mCursor; }
    void SortByValue() override final
    {
        std::sort(mKVTupleVec.begin(), mKVTupleVec.end(), ValueCompare);
        mCursor = 0;
    }
    bool IsDeleted() const override final
    {
        assert(IsValid());
        return mKVTupleVec[mCursor].IsDeleted();
    }

    uint64_t GetKey() const override final { return Key(); }
    autil::StringView GetValue() const override final
    {
        assert(IsValid());
        return autil::StringView((const char*)mKVTupleVec[mCursor].ValueAddr(), sizeof(_VT));
    }

    bool Seek(int64_t offset) override final
    {
        if (mKVTupleVec.empty()) {
            return true;
        }
        if (offset >= (int64_t)mKVTupleVec.size()) {
            return false;
        }
        mCursor = offset;
        return true;
    }
    void Reset() override final { mCursor = 0; }
    virtual int64_t GetOffset() const override final { return mCursor; }

public:
    void SortByKey() override final
    {
        std::sort(mKVTupleVec.begin(), mKVTupleVec.end(), KeyCompare);
        mCursor = 0;
    }
    const _KT& Key() const
    {
        assert(IsValid());
        return mKVTupleVec[mCursor].Key();
    }
    const _VT& Value() const
    {
        assert(IsValid());
        return mKVTupleVec[mCursor].Value();
    }

private:
    static bool KeyCompare(const KVTuple& left, const KVTuple& right) { return left.key < right.key; }

    static bool ValueCompare(const KVTuple& left, const KVTuple& right)
    {
        // Rule: Delete < Insert
        if (left.IsDeleted()) {
            return right.IsDeleted() ? KeyCompare(left, right) : true;
        } else if (right.IsDeleted()) {
            return false;
        }
        // all not Delete
        if (left.value == right.value) {
            return KeyCompare(left, right);
        }
        return left.value < right.value;
    }

protected:
    KVTupleVec mKVTupleVec;
    int64_t mCursor;

protected:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE6_CUSTOM(typename, typename, typename, bool, bool, bool, util, ClosedHashTableFileIterator);

////////////////////////////////////////////////////////////////////////
// hide some methods
template <typename HashTableHeader, typename _KT, typename _VT, bool hasDeleted, bool useCompactBucket>
class ClosedHashTableFileIterator<HashTableHeader, _KT, _VT, true, hasDeleted, useCompactBucket>
    : public ClosedHashTableFileIterator<HashTableHeader, _KT, _VT, false, hasDeleted, useCompactBucket>
{
private:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;
    typedef ClosedHashTableFileIterator<HashTableHeader, _KT, _VT, false, hasDeleted, useCompactBucket> Base;
    using Base::_logger;
    using Base::mKVTupleVec;

public:
    bool Init(const file_system::FileReaderPtr& fileReader) override
    {
        Base::Init(fileReader);
        for (size_t i = 0; i < 2; ++i) {
            SpecialBucket bucket;
            if (sizeof(bucket) != fileReader->Read(&bucket, sizeof(bucket)).GetOrThrow()) {
                IE_LOG(ERROR, "read bucket[%lu] failed", i);
                return false;
            }
            if (!bucket.IsEmpty()) {
                typename Base::KVTuple tuple(bucket.Key(), bucket.Value(), bucket.IsDeleted());
                mKVTupleVec.push_back(tuple);
            }
        }
        return true;
    }
};
}} // namespace indexlib::common

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

// FORMAT: see dense_hash_table.h or cuckoo_hash_table.h
template <typename HashTableHeader, typename _KT, typename _VT,
          bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey, bool useCompactBucket = false>
class ClosedHashTableBufferedFileIterator : public HashTableFileIterator
{
protected:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    struct KVTuple {
        _KT key;
        _VT value;
        bool deleted;
    };

public:
    size_t EstimateMemoryUse(size_t keyCount) const override final { return 0; }

public:
    bool Init(const file_system::FileReaderPtr& fileReader) override
    {
        return DoInit(fileReader, fileReader->GetLogicLength());
    }
    size_t Size() const override { return mKeyCount; }
    bool IsValid() const override { return mIsValid; }
    void MoveToNext() override
    {
        assert(IsValid());
        DoMoveToNext<Bucket>(mLength);
    }
    bool IsDeleted() const override
    {
        assert(IsValid());
        return mKVTuple.deleted;
    }
    uint64_t GetKey() const override final { return Key(); }
    autil::StringView GetValue() const override final
    {
        assert(IsValid());
        return autil::StringView((const char*)std::addressof(mKVTuple.value), sizeof(mKVTuple.value));
    }
    void SortByKey() override final { assert(false); }
    void SortByValue() override final { assert(false); }
    bool Seek(int64_t offset) override
    {
        if (mLength == 0) {
            return true;
        }
        if (offset >= mLength) {
            return false;
        }
        mOffset = offset;
        if (0 == offset) {
            Reset();
        }
        return true;
    }
    void Reset() override
    {
        mIsValid = true;
        mOffset = sizeof(HashTableHeader);
        MoveToNext();
    }
    virtual int64_t GetOffset() const override final { return mOffset; }

public:
    const _KT& Key() const
    {
        assert(IsValid());
        return mKVTuple.key;
    }
    const _VT& Value() const
    {
        assert(IsValid());
        return mKVTuple.value;
    }

protected:
    bool DoInit(const file_system::FileReaderPtr& fileReader, size_t length)
    {
        mFileReader = fileReader;
        mLength = length;
        HashTableHeader header;
        if (sizeof(header) != fileReader->Read(&header, sizeof(header), 0).GetOrThrow()) {
            IE_LOG(ERROR,
                   "read header failed from file[%s], "
                   "headerSize[%lu], fileLength[%lu]",
                   fileReader->DebugString().c_str(), sizeof(header), fileReader->GetLogicLength());
            return false;
        }
        mKeyCount = header.keyCount;
        size_t bucketCount = header.bucketCount;
        if (sizeof(Bucket) * bucketCount + sizeof(header) != mLength) {
            IE_LOG(ERROR, "invalid file[%s], size[%lu], headerSize[%lu], length[%lu]",
                   fileReader->DebugString().c_str(), fileReader->GetLogicLength(), sizeof(header), length);
            return false;
        }
        Reset();
        return true;
    }
    template <typename Bucket>
    void DoMoveToNext(size_t endOffset)
    {
        Bucket bucket;
        while (bucket.IsEmpty()) {
            if (mOffset >= endOffset) {
                mIsValid = false;
                return;
            }
            if (sizeof(bucket) != mFileReader->Read(&bucket, sizeof(bucket), mOffset).GetOrThrow()) {
                IE_LOG(ERROR, "read bucket failed from file[%s], offset[%lu]", mFileReader->DebugString().c_str(),
                       mOffset);
                return;
            }
            mOffset += sizeof(Bucket);
        }
        mKVTuple.key = bucket.Key();
        mKVTuple.value = bucket.Value();
        mKVTuple.deleted = bucket.IsDeleted();
    }

protected:
    file_system::FileReaderPtr mFileReader;
    size_t mOffset = 0;
    size_t mLength = 0;
    int64_t mKeyCount = 0;
    KVTuple mKVTuple;
    bool mIsValid = false;

protected:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE5_CUSTOM(typename, typename, typename, bool, bool, util, ClosedHashTableBufferedFileIterator);

////////////////////////////////////////////////////////////////////////
// hide some methods
template <typename HashTableHeader, typename _KT, typename _VT, bool useCompactBucket>
class ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, true, useCompactBucket>
    : public ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, false, useCompactBucket>
{
private:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;
    typedef ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, false, useCompactBucket> Base;
    using Base::_logger;

public:
    bool Init(const file_system::FileReaderPtr& fileReader) override
    {
        mFileLength = fileReader->GetLogicLength();
        if (mFileLength <= sizeof(SpecialBucket) * 2 + sizeof(HashTableHeader)) {
            IE_LOG(ERROR, "invalid file size[%lu], headerSize[%lu]", fileReader->GetLogicLength(),
                   sizeof(HashTableHeader));
            return false;
        }
        if (!Base::DoInit(fileReader, mFileLength - sizeof(SpecialBucket) * 2)) {
            return false;
        }
        if (!IsValid()) {
            Base::mIsValid = true;
            MoveToNext();
        }
        return true;
    }
    bool IsValid() const override { return Base::mIsValid; }
    bool Seek(int64_t offset) override
    {
        if (Base::mLength == 0 || mFileLength == 0) {
            return true;
        }
        if (offset > Base::mLength && offset > mFileLength) {
            return false;
        }
        Base::mOffset = offset;
        if (0 == offset) {
            Reset();
        }
        return true;
    }
    void Reset() override
    {
        Base::mIsValid = true;
        Base::mOffset = sizeof(HashTableHeader);
        MoveToNext();
    }
    void MoveToNext() override
    {
        assert(IsValid());
        if (likely(Base::mOffset < Base::mLength)) {
            Base::template DoMoveToNext<typename Base::Bucket>(Base::mLength);
            if (likely(Base::mIsValid)) {
                return;
            }
            Base::mIsValid = true;
        }
        return Base::template DoMoveToNext<SpecialBucket>(mFileLength);
    }

private:
    size_t mFileLength = 0;
};
}} // namespace indexlib::common

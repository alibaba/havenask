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
#ifndef __INDEXLIB_CLOSED_HASH_TABLE_ITERATOR_H
#define __INDEXLIB_CLOSED_HASH_TABLE_ITERATOR_H

#include <algorithm>
#include <cassert>

#include "indexlib/common_define.h"

namespace indexlib { namespace common {

template <typename _KT, typename _VT, typename Bucket, bool HasSpecialKey>
class ClosedHashTableIterator
{
public:
    typedef _VT ValueType;

public:
    ClosedHashTableIterator(Bucket* bucket, uint64_t bucketCount, uint64_t keyCount)
        : ClosedHashTableIterator(bucket, bucketCount, keyCount, true)
    {
    }
    ~ClosedHashTableIterator() {}

protected:
    ClosedHashTableIterator(Bucket* bucket, uint64_t bucketCount, uint64_t keyCount, bool needMoveToNext)
        : mBucket(bucket)
        , mBucketCount((int64_t)bucketCount)
        , mKeyCount(keyCount)
        , mCursor(-1)
    {
        assert(mBucketCount > 0);
        if (needMoveToNext) {
            MoveToNext();
        }
    }

public:
    size_t Size() const { return mKeyCount; }

    bool IsValid() const { return mCursor < mBucketCount; }
    void Reset()
    {
        mCursor = -1;
        MoveToNext();
    }
    void MoveToNext()
    {
        assert(IsValid());
        ++mCursor;
        while (IsValid() && mBucket[mCursor].IsEmpty()) {
            ++mCursor;
        }
    }

    void SortByKey()
    {
        assert(mBucketCount > 0);
        Bucket* begin = &(mBucket[0]);
        Bucket* end = begin + mBucketCount;
        std::sort(begin, end, KeyCompare);
        mCursor = -1;
        MoveToNext();
    }

    void SortByValue()
    {
        assert(mBucketCount > 0);
        Bucket* begin = &(mBucket[0]);
        Bucket* end = begin + mBucketCount;
        std::sort(begin, end, ValueCompare);
        mCursor = -1;
        MoveToNext();
    }

    bool IsDeleted() const
    {
        assert(IsValid());
        return mBucket[mCursor].IsDeleted();
    }

    const _KT& Key() const
    {
        assert(IsValid());
        return mBucket[mCursor].Key();
    }
    const _VT& Value() const
    {
        assert(IsValid());
        return mBucket[mCursor].Value();
    }

    void SetValue(const _VT& value) { mBucket[mCursor].UpdateValue(value); }

private:
    static bool KeyCompare(const Bucket& left, Bucket& right) { return left.Key() < right.Key(); }

    static bool ValueCompare(const Bucket& left, Bucket& right)
    {
        if (left.IsDeleted()) {
            return right.IsDeleted() ? KeyCompare(left, right) : true;
        } else if (right.IsDeleted()) {
            return false;
        }
        if (left.Value() == right.Value()) {
            return KeyCompare(left, right);
        }
        return left.Value() < right.Value();
    }

protected:
    Bucket* mBucket;
    int64_t mBucketCount;
    uint64_t mKeyCount;
    int64_t mCursor;
};

//////////////////////////////////////////////////////////////////////
template <typename _KT, typename _VT, typename Bucket>
class ClosedHashTableIterator<_KT, _VT, Bucket, true> : public ClosedHashTableIterator<_KT, _VT, Bucket, false>
{
private:
    typedef ClosedHashTableIterator<_KT, _VT, Bucket, false> Base;
    using Base::mBucket;
    using Base::mBucketCount;
    using Base::mCursor;

public:
    ClosedHashTableIterator(Bucket* bucket, uint64_t bucketCount, uint64_t keyCount)
        : Base(bucket, bucketCount, keyCount, false)
    {
        MoveToNext();
    }

public:
    bool IsValid() const { return Base::IsValid() || mCursor == mBucketCount || mCursor == mBucketCount + 1; }

    void MoveToNext()
    {
        if (likely(mCursor < mBucketCount)) {
            Base::MoveToNext();
            if (likely(mCursor < mBucketCount)) {
                return;
            }
        } else {
            ++mCursor;
        }
        if (mCursor == mBucketCount && EmptyBucket()->IsEmpty()) {
            ++mCursor;
        }
        if (mCursor == mBucketCount + 1 && DeleteBucket()->IsEmpty()) {
            ++mCursor;
        }
    }

    void SortByKey() { assert(false); }

    void SortByValue() { assert(false); }

    bool IsDeleted() const
    {
        if (likely(mCursor < mBucketCount)) {
            return Base::IsDeleted();
        } else if (mCursor == mBucketCount) {
            return EmptyBucket()->IsDeleted();
        }
        assert(mCursor == mBucketCount + 1);
        return DeleteBucket()->IsDeleted();
    }

    const _KT& Key() const
    {
        if (likely(mCursor < mBucketCount)) {
            return Base::Key();
        } else if (mCursor == mBucketCount) {
            return EmptyBucket()->Key();
        }
        assert(mCursor == mBucketCount + 1);
        return DeleteBucket()->Key();
    }

    const _VT& Value() const
    {
        if (mCursor < mBucketCount) {
            return Base::Value();
        } else if (mCursor == mBucketCount) {
            return EmptyBucket()->Value();
        }
        assert(mCursor == mBucketCount + 1);
        return DeleteBucket()->Value();
    }

private:
    typedef typename Bucket::SpecialBucket SpecialBucket;

    const SpecialBucket* EmptyBucket() const { return reinterpret_cast<const SpecialBucket*>(&mBucket[mBucketCount]); }

    const SpecialBucket* DeleteBucket() const
    {
        return reinterpret_cast<const SpecialBucket*>(&mBucket[mBucketCount]) + 1;
    }
};
}} // namespace indexlib::common

#endif //__INDEXLIB_CLOSED_HASH_TABLE_ITERATOR_H

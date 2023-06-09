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
#include <algorithm>
#include <cassert>
#include <cstdint>

#include "autil/CommonMacros.h"

namespace indexlibv2::index {

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
        : _bucket(bucket)
        , _bucketCount((int64_t)bucketCount)
        , _keyCount(keyCount)
        , _cursor(-1)
    {
        assert(_bucketCount > 0);
        if (needMoveToNext) {
            MoveToNext();
        }
    }

public:
    size_t Size() const { return _keyCount; }

    bool IsValid() const { return _cursor < _bucketCount; }
    void Reset()
    {
        _cursor = -1;
        MoveToNext();
    }
    void MoveToNext()
    {
        assert(IsValid());
        ++_cursor;
        while (IsValid() && _bucket[_cursor].IsEmpty()) {
            ++_cursor;
        }
    }

    void SortByKey()
    {
        assert(_bucketCount > 0);
        Bucket* begin = &(_bucket[0]);
        Bucket* end = begin + _bucketCount;
        std::sort(begin, end, KeyCompare);
        _cursor = -1;
        MoveToNext();
    }

    void SortByValue()
    {
        assert(_bucketCount > 0);
        Bucket* begin = &(_bucket[0]);
        Bucket* end = begin + _bucketCount;
        std::sort(begin, end, ValueCompare);
        _cursor = -1;
        MoveToNext();
    }

    bool IsDeleted() const
    {
        assert(IsValid());
        return _bucket[_cursor].IsDeleted();
    }

    const _KT& Key() const
    {
        assert(IsValid());
        return _bucket[_cursor].Key();
    }
    const _VT& Value() const
    {
        assert(IsValid());
        return _bucket[_cursor].Value();
    }

    void SetValue(const _VT& value) { _bucket[_cursor].UpdateValue(value); }

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
    Bucket* _bucket;
    int64_t _bucketCount;
    uint64_t _keyCount;
    int64_t _cursor;
};

//////////////////////////////////////////////////////////////////////
template <typename _KT, typename _VT, typename Bucket>
class ClosedHashTableIterator<_KT, _VT, Bucket, true> : public ClosedHashTableIterator<_KT, _VT, Bucket, false>
{
private:
    typedef ClosedHashTableIterator<_KT, _VT, Bucket, false> Base;
    using Base::_bucket;
    using Base::_bucketCount;
    using Base::_cursor;

public:
    ClosedHashTableIterator(Bucket* bucket, uint64_t bucketCount, uint64_t keyCount)
        : Base(bucket, bucketCount, keyCount, false)
    {
        MoveToNext();
    }

public:
    bool IsValid() const { return Base::IsValid() || _cursor == _bucketCount || _cursor == _bucketCount + 1; }

    void MoveToNext()
    {
        if (likely(_cursor < _bucketCount)) {
            Base::MoveToNext();
            if (likely(_cursor < _bucketCount)) {
                return;
            }
        } else {
            ++_cursor;
        }
        if (_cursor == _bucketCount && EmptyBucket()->IsEmpty()) {
            ++_cursor;
        }
        if (_cursor == _bucketCount + 1 && DeleteBucket()->IsEmpty()) {
            ++_cursor;
        }
    }

    void SortByKey() { assert(false); }

    void SortByValue() { assert(false); }

    bool IsDeleted() const
    {
        if (likely(_cursor < _bucketCount)) {
            return Base::IsDeleted();
        } else if (_cursor == _bucketCount) {
            return EmptyBucket()->IsDeleted();
        }
        assert(_cursor == _bucketCount + 1);
        return DeleteBucket()->IsDeleted();
    }

    const _KT& Key() const
    {
        if (likely(_cursor < _bucketCount)) {
            return Base::Key();
        } else if (_cursor == _bucketCount) {
            return EmptyBucket()->Key();
        }
        assert(_cursor == _bucketCount + 1);
        return DeleteBucket()->Key();
    }

    const _VT& Value() const
    {
        if (_cursor < _bucketCount) {
            return Base::Value();
        } else if (_cursor == _bucketCount) {
            return EmptyBucket()->Value();
        }
        assert(_cursor == _bucketCount + 1);
        return DeleteBucket()->Value();
    }

private:
    typedef typename Bucket::SpecialBucket SpecialBucket;

    const SpecialBucket* EmptyBucket() const { return reinterpret_cast<const SpecialBucket*>(&_bucket[_bucketCount]); }

    const SpecialBucket* DeleteBucket() const
    {
        return reinterpret_cast<const SpecialBucket*>(&_bucket[_bucketCount]) + 1;
    }
};
} // namespace indexlibv2::index

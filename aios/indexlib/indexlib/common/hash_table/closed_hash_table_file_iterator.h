#ifndef __INDEXLIB_CLOSED_HASH_TABLE_FILE_ITERATOR_H
#define __INDEXLIB_CLOSED_HASH_TABLE_FILE_ITERATOR_H


#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"

IE_NAMESPACE_BEGIN(common);

template<typename _KT, typename _VT, bool hasDeleted>
struct ClosedHashTableKVTuple;

template<typename _KT, typename _VT>
struct ClosedHashTableKVTuple<_KT, _VT, true>
{
    _KT key;
    _VT value;
    bool deleted;
    ClosedHashTableKVTuple(const _KT& _key, const _VT& _value, bool _deleted)
        : key(_key), value(_value), deleted(_deleted) {}
    const _KT& Key() const { return key; }
    const _VT& Value() const { return value; }
    bool IsDeleted() const { return deleted; }
};

template<typename _KT, typename _VT>
struct ClosedHashTableKVTuple<_KT, _VT, false>
{
    _KT key;
    _VT value;
    ClosedHashTableKVTuple(const _KT& _key, const _VT& _value, bool _deleted)
        : key(_key), value(_value) {}
    const _KT& Key() const { return key; }
    const _VT& Value() const { return value; }
    bool IsDeleted() const { return false; }
};

// FORMAT: see dense_hash_table.h or cuckoo_hash_table.h
template<typename HashTableHeader, typename _KT, typename _VT,
         bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
         bool hasDeleted = true,
         bool useCompactBucket = false>
class ClosedHashTableFileIterator
{
public:
    ClosedHashTableFileIterator() : mCursor(0) {}
    ~ClosedHashTableFileIterator() = default;

protected:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    typedef ClosedHashTableKVTuple<_KT, _VT, hasDeleted> KVTuple;
    typedef std::vector<KVTuple> KVTupleVec;

public:
    static size_t EstimateMemoryUse(size_t keyCount)
    { return sizeof(KVTuple) * keyCount; }

public:
    bool Init(const file_system::FileReaderPtr& fileReader)
    {
        HashTableHeader header;
        if (sizeof(header) != fileReader->Read(&header, sizeof(header)))
        {
            IE_LOG(ERROR, "read header failed, header size[%lu], fileLength[%lu]",
                   sizeof(header), fileReader->GetLength());
            return false;
        }
        IE_LOG(INFO, "iterator reserve size [%lu]", header.keyCount * sizeof(KVTuple));
        mKVTupleVec.reserve(header.keyCount);
        for (size_t i = 0; i < header.bucketCount; ++i)
        {
            Bucket bucket;
            if (sizeof(bucket) != fileReader->Read(&bucket, sizeof(bucket)))
            {
                IE_LOG(ERROR, "read bucket[%lu] failed", i);
                return false;
            }
            if (!bucket.IsEmpty())
            {
                KVTuple tuple(bucket.Key(), bucket.Value(), bucket.IsDeleted());
                mKVTupleVec.push_back(tuple);
            }
        }
        return true;
    }

    size_t Size() const { return mKVTupleVec.size(); }
    bool IsValid() const { return mCursor < (int64_t)mKVTupleVec.size(); }
    void MoveToNext() { ++mCursor; }

    void Reset() { mCursor = 0; }
    void SortByKey()
    {
        std::sort(mKVTupleVec.begin(), mKVTupleVec.end(), KeyCompare);
        mCursor = 0;
    }
    void SortByValue()
    {
        std::sort(mKVTupleVec.begin(), mKVTupleVec.end(), ValueCompare);
        mCursor = 0;
    }

    const _KT& Key() const
    { assert(IsValid()); return mKVTupleVec[mCursor].Key(); }
    const _VT& Value() const
    { assert(IsValid()); return mKVTupleVec[mCursor].Value(); }
    bool IsDeleted() const
    { assert(IsValid()); return mKVTupleVec[mCursor].IsDeleted(); }

private:
    static bool KeyCompare(const KVTuple& left, const KVTuple& right)
    { return left.key < right.key; }

    static bool ValueCompare(const KVTuple& left, const KVTuple& right)
    {
        // Rule: Delete < Insert
        if (left.deleted)
        {
            return right.deleted ? KeyCompare(left, right) : true;
        }
        else if (right.deleted)
        {
            return false;
        }
        // all not Delete
        if (left.value == right.value)
        {
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
IE_LOG_SETUP_TEMPLATE6_CUSTOM(typename, typename, typename, bool, bool, bool,
                              util, ClosedHashTableFileIterator);

////////////////////////////////////////////////////////////////////////
// hide some methods
template<typename HashTableHeader, typename _KT, typename _VT, bool hasDeleted, bool useCompactBucket>
class ClosedHashTableFileIterator<HashTableHeader, _KT, _VT, true, hasDeleted, useCompactBucket> :
    public ClosedHashTableFileIterator<HashTableHeader, _KT, _VT, false, hasDeleted, useCompactBucket>
{
private:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;
    typedef ClosedHashTableFileIterator<HashTableHeader, _KT, _VT, false, hasDeleted, useCompactBucket> Base;
    using Base::mKVTupleVec;
    using Base::_logger;

public:
    bool Init(const file_system::FileReaderPtr& fileReader)
    {
        Base::Init(fileReader);
        for (size_t i = 0; i < 2; ++i)
        {
            SpecialBucket bucket;
            if (sizeof(bucket) != fileReader->Read(&bucket, sizeof(bucket)))
            {
                IE_LOG(ERROR, "read bucket[%lu] failed", i);
                return false;
            }
            if (!bucket.IsEmpty())
            {
                typename Base::KVTuple tuple(bucket.Key(), bucket.Value(), bucket.IsDeleted());
                mKVTupleVec.push_back(tuple);
            }
        }
        return true;
    }
};

IE_NAMESPACE_END(common);
#endif //__INDEXLIB_CLOSED_HASH_TABLE_FILE_ITERATOR_H

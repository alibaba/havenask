#ifndef __INDEXLIB_CLOSED_HASH_TABLE_BUFFERED_FILE_ITERATOR_H
#define __INDEXLIB_CLOSED_HASH_TABLE_BUFFERED_FILE_ITERATOR_H


#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"

IE_NAMESPACE_BEGIN(common);

// FORMAT: see dense_hash_table.h or cuckoo_hash_table.h
template<typename HashTableHeader, typename _KT, typename _VT,
         bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
         bool useCompactBucket = false>
class ClosedHashTableBufferedFileIterator
{
// public:
//     ClosedHashTableBufferedFileIterator() = default;
//     ~ClosedHashTableBufferedFileIterator() = default;

protected:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    struct KVTuple
    {
        _KT key;
        _VT value;
        bool deleted;
    };

public:
    static size_t EstimateMemoryUse(size_t keyCount)
    { return 0; }

public:
    bool Init(const file_system::FileReaderPtr& fileReader)
    { return DoInit(fileReader, fileReader->GetLength()); }
    size_t Size() const { return mKeyCount; }
    bool IsValid() const { return mIsValid; }
    void MoveToNext() { assert(IsValid()); DoMoveToNext<Bucket>(mLength); }
    void Reset()
    {
        mIsValid = true;
        mOffset = sizeof(HashTableHeader);
        MoveToNext();
    }

    const _KT& Key() const { assert(IsValid()); return mKVTuple.key; }
    const _VT& Value() const { assert(IsValid()); return mKVTuple.value; }
    bool IsDeleted() const { assert(IsValid()); return mKVTuple.deleted; }

protected:
    bool DoInit(const file_system::FileReaderPtr& fileReader, size_t length)
    {
        mFileReader = fileReader;
        mLength = length;
        HashTableHeader header;
        if (sizeof(header) != fileReader->Read(&header, sizeof(header), 0))
        {
            IE_LOG(ERROR, "read header failed from file[%s], "
                   "headerSize[%lu], fileLength[%lu]",
                   fileReader->GetPath().c_str(), sizeof(header),
                   fileReader->GetLength());
            return false;
        }
        mKeyCount = header.keyCount;
        size_t bucketCount = header.bucketCount;
        if (sizeof(Bucket) * bucketCount + sizeof(header) != mLength)
        {
            IE_LOG(ERROR, "invalid file[%s], size[%lu], headerSize[%lu], length[%lu]",
                   fileReader->GetPath().c_str(), fileReader->GetLength(),
                   sizeof(header), length);
            return false;
        }
        Reset();
        return true;
    }
    template<typename Bucket>
    void DoMoveToNext(size_t endOffset)
    {
        Bucket bucket;
        while(bucket.IsEmpty())
        {
            if (mOffset >= endOffset)
            {
                mIsValid = false;
                return;
            }
            if (sizeof(bucket) != mFileReader->Read(&bucket, sizeof(bucket), mOffset))
            {
                IE_LOG(ERROR, "read bucket failed from file[%s], offset[%lu]",
                       mFileReader->GetPath().c_str(), mOffset);
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
IE_LOG_SETUP_TEMPLATE5_CUSTOM(typename, typename, typename, bool, bool,
                              util, ClosedHashTableBufferedFileIterator);

////////////////////////////////////////////////////////////////////////
// hide some methods
template<typename HashTableHeader, typename _KT, typename _VT, bool useCompactBucket>
class ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, true, useCompactBucket> :
    public ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, false, useCompactBucket>
{
private:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;
    typedef ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, false, useCompactBucket> Base;
    using Base::_logger;

public:
    bool Init(const file_system::FileReaderPtr& fileReader)
    {
        mFileLength = fileReader->GetLength();
        if (mFileLength <= sizeof(SpecialBucket) * 2 + sizeof(HashTableHeader))
        {
            IE_LOG(ERROR, "invalid file size[%lu], headerSize[%lu]",
                   fileReader->GetLength(), sizeof(HashTableHeader));
            return false;
        }
        if (!Base::DoInit(fileReader, mFileLength - sizeof(SpecialBucket) * 2))
        {
            return false;
        }
        if (!IsValid())
        {
            Base::mIsValid = true;
            MoveToNext();
        }
        return true;
    }
    bool IsValid() const { return Base::mIsValid; }
    void MoveToNext()
    {
        assert(IsValid());
        if (likely(Base::mOffset < Base::mLength))
        {
            Base::template DoMoveToNext<typename Base::Bucket>(Base::mLength);
            if (likely(Base::mIsValid))
            {
                return;
            }
            Base::mIsValid = true;
        }
        return Base::template DoMoveToNext<SpecialBucket>(mFileLength);
    }
    void Reset()
    {
        Base::mIsValid = true;
        Base::mOffset = sizeof(HashTableHeader);
        MoveToNext();
    }
private:
    size_t mFileLength = 0;
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CLOSED_HASH_TABLE_BUFFERED_FILE_ITERATOR_H

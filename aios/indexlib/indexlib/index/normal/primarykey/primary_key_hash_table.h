#ifndef __INDEXLIB_PRIMARY_KEY_HASH_TABLE_H
#define __INDEXLIB_PRIMARY_KEY_HASH_TABLE_H

#include <cmath>
#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/prime_number_table.h"
#include "indexlib/util/hash_util.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class PrimaryKeyHashTable
{
public:
    typedef PKPair<Key> TypedPKPair;
public:
    // for load from buffer
    PrimaryKeyHashTable()
        : mPkCountPtr(NULL)
        , mDocCountPtr(NULL)
        , mBucketCountPtr(NULL)
        , mBucketPtr(NULL)
        , mPkPairPtr(NULL)
        , mBucketCount(0) {}
    
    ~PrimaryKeyHashTable() {}
public:
    // for read from buffer
    void Init(char* buffer)
    {
        assert(buffer);
        mPkCountPtr = (uint64_t*)buffer;
        mDocCountPtr = mPkCountPtr + 1;
        mBucketCountPtr = mDocCountPtr + 1;
        mBucketCount = *mBucketCountPtr;
        mPkPairPtr = (TypedPKPair*)(mBucketCountPtr + 1);
        mBucketPtr = (docid_t*)(mPkPairPtr + *mDocCountPtr);
    }

    // for write to buffer
    void Init(char* buffer, uint64_t pkCount, uint64_t docCount)
    {
        assert(buffer);
        mPkCountPtr = (uint64_t*)buffer;
        *mPkCountPtr = pkCount;
        mDocCountPtr = mPkCountPtr + 1;
        *mDocCountPtr = docCount;
        mBucketCountPtr = mDocCountPtr + 1;
        *mBucketCountPtr = GetBucketCount(pkCount);
        mBucketCount = *mBucketCountPtr;
        mPkPairPtr = (TypedPKPair*)(mBucketCountPtr + 1);
        mBucketPtr = (docid_t*)(mPkPairPtr + *mDocCountPtr);
        
        InitBucket();
        InitPkPairs(docCount);
    }
    
    void Insert(const TypedPKPair& pkPair)
    {
        util::KeyHash<Key> hashFun;
        uint64_t bucketIdx = hashFun(pkPair.key) % mBucketCount;
        mPkPairPtr[pkPair.docid].key = pkPair.key;
        mPkPairPtr[pkPair.docid].docid = mBucketPtr[bucketIdx];
        mBucketPtr[bucketIdx] = pkPair.docid;
    }

    docid_t Find(const Key& key) const
    {
        //for uint128_t mod
        util::KeyHash<Key> hashFun;
        uint64_t bucketIdx = hashFun(key) % mBucketCount;
        docid_t next = mBucketPtr[bucketIdx];
        while (next != INVALID_DOCID)
        {
            const TypedPKPair& pkPair = mPkPairPtr[next];
            if (likely(pkPair.key == key))
            {
                return next;
            }
            next = pkPair.docid;
        }
        return INVALID_DOCID;
    }

public:
    static uint64_t SeekToPkPair(const file_system::FileReaderPtr& fileReader)
    {
        assert(fileReader);
        uint64_t pkCount;
        uint64_t docCount;
        uint64_t bucketCount;
        fileReader->Read(&pkCount, sizeof(uint64_t));
        fileReader->Read(&docCount, sizeof(uint64_t));
        fileReader->Read(&bucketCount, sizeof(uint64_t));
        return pkCount;
    }
    static size_t CalculateMemorySize(uint64_t pkCount, uint64_t docCount);
    static bool IsInvalidPkPair(const TypedPKPair& pkPair);
    static size_t EstimatePkCount(size_t fileLength, uint32_t docCount);
    static size_t EstimateMemoryCostPerDoc() 
    { return sizeof(TypedPKPair) + 2 * sizeof(docid_t); }

private:
    static uint64_t GetBucketCount(size_t pkCount)
    {
        return util::PrimeNumberTable::FindPrimeNumber(
            int64_t(pkCount * BUCKET_COUNT_FACTOR)); 
    }

    void InitBucket()
    {
        for (uint64_t i = 0; i < mBucketCount; ++i)
        {
            mBucketPtr[i] = INVALID_DOCID;
        }
    }

    void InitPkPairs(uint64_t docCount)
    {
        if (*mPkCountPtr < docCount)
        {
            for (docid_t docid = 0; (size_t)docid < docCount; ++docid)
            {
                mPkPairPtr[docid] = INVALID_PK_PAIR;
            }
        }
    }

private:
    static docid_t NON_EXIST_DOCID;
    static TypedPKPair INVALID_PK_PAIR;
    static double BUCKET_COUNT_FACTOR;
    
public:
    uint64_t* mPkCountPtr;
    uint64_t* mDocCountPtr;
    uint64_t* mBucketCountPtr;
    docid_t* mBucketPtr;
    TypedPKPair* mPkPairPtr;
    uint64_t mBucketCount; // cache for performance

private:
    friend class PrimaryKeyHashTableTest;
};

//////////////////////////////////////////////////////////////////////

template <typename Key>
docid_t PrimaryKeyHashTable<Key>::NON_EXIST_DOCID = (docid_t)-2;

template <typename Key>
typename PrimaryKeyHashTable<Key>::TypedPKPair PrimaryKeyHashTable<Key>::INVALID_PK_PAIR =
{ Key(), NON_EXIST_DOCID };

template <typename Key>
double PrimaryKeyHashTable<Key>::BUCKET_COUNT_FACTOR = (double)5/3;

template <typename Key>
size_t PrimaryKeyHashTable<Key>::CalculateMemorySize(uint64_t pkCount, uint64_t docCount)
{
    // pk count, doc count, bucket count
    size_t headerSize = sizeof(uint64_t) * 3;
    size_t pkPairVecSize = sizeof(TypedPKPair) * docCount;
    size_t bucketSize = sizeof(docid_t) * GetBucketCount(pkCount);
    return headerSize + pkPairVecSize + bucketSize;
}

template <typename Key>
bool PrimaryKeyHashTable<Key>::IsInvalidPkPair(const TypedPKPair& pkPair)
{
    return pkPair.docid == NON_EXIST_DOCID;
}

template <typename Key>
size_t PrimaryKeyHashTable<Key>::EstimatePkCount(size_t fileLength, uint32_t docCount)
{
    size_t headerSize = sizeof(uint64_t) * 3;
    size_t pkPairVecSize = sizeof(TypedPKPair) * docCount;
    size_t bucketSize = fileLength - headerSize - pkPairVecSize;
    return std::min((size_t)std::ceil(bucketSize / BUCKET_COUNT_FACTOR), (size_t)docCount);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_HASH_TABLE_H

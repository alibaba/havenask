#ifndef __INDEXLIB_CUCKOO_HASH_TABLE_FILE_READER_H
#define __INDEXLIB_CUCKOO_HASH_TABLE_FILE_READER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"

IE_NAMESPACE_BEGIN(common);

// FORMAT: see cuckoo_hash_table.h
template<typename _KT, typename _VT,
         bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
         bool useCompactBucket = false>
class CuckooHashTableFileReader
{
public:
    CuckooHashTableFileReader() = default;
    ~CuckooHashTableFileReader() = default;

private:
    typedef CuckooHashTable<_KT, _VT, HasSpecialKey, useCompactBucket> HashTable;
    typedef typename HashTable::HashTableHeader HashTableHeader;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;

private:
    bool ReadHeader(const file_system::DirectoryPtr &directory,
                    const file_system::FileReaderPtr &fileReader) {
        size_t size = fileReader->GetLength();
        HashTableHeader header;
        if (size < sizeof(header))
        {
            IE_LOG(ERROR, "read header failed, header size[%lu], fileLength[%lu]",
                   sizeof(header), fileReader->GetLength());
            return false;
        }
        std::string fileName;
        util::PathUtil::GetRelativePath(directory->GetPath(),
                                        fileReader->GetPath(), fileName);
        std::string sliceFileName = fileName + ".header";
        auto sliceFile = directory->GetSliceFile(sliceFileName);
        if (sliceFile)
        {
            mHeaderReader = sliceFile->CreateSliceFileReader();
        }
        else
        {
            size_t sliceLen = sizeof(header);
            sliceFile = directory->CreateSliceFile(sliceFileName, sliceLen, 1);
            sliceFile->ReserveSliceFile(sliceLen);
            mHeaderReader = sliceFile->CreateSliceFileReader();
            if (sliceLen !=
                fileReader->Read(mHeaderReader->GetBaseAddress(), sliceLen, 0)) {
                IE_LOG(ERROR, "read header failed, header size[%lu], fileLength[%lu]",
                       sizeof(header), fileReader->GetLength());
                return false;
            }
        }
        header = *(HashTableHeader *)mHeaderReader->GetBaseAddress();
        mBucketCount = header.bucketCount;
        mKeyCount = header.keyCount;
        mNumHashFunc = header.numHashFunc;
        if (size < sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket))
        {
            IE_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]",
                   size, sizeof(HashTableHeader), mBucketCount * sizeof(Bucket));
            return false;
        }
        mBlockCount = (size - sizeof(HashTableHeader) - (HasSpecialKey ? sizeof(Bucket) * 2 : 0))
                      / sizeof(Bucket) / HashTable::BLOCK_SIZE;
        if (mBlockCount < 1)
        {
            IE_LOG(ERROR, "size[%lu] too small", size);
            return false;
        }
        return true;
    }

public:
    bool Init(const file_system::DirectoryPtr& directory,
              const file_system::FileReaderPtr& fileReader)
    {
        if (!ReadHeader(directory, fileReader))
        {
            return false;
        }
        mFileReader = fileReader.get();
        return true;
    }

    misc::Status Find(
        const _KT& key, _VT& value, file_system::BlockAccessCounter* blockCounter) const
    {
        file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.advice = storage::IO_ADVICE_LOW_LATENCY;        
        for (uint32_t hashCnt = 0; hashCnt < mNumHashFunc; ++hashCnt)
        {
            const _KT& hash = HashTable::CuckooHash(key, hashCnt);
            uint64_t bucketId = HashTable::GetFirstBucketIdInBlock(hash, mBlockCount);
            Bucket block[HashTable::BLOCK_SIZE];
            mFileReader->Read(
                block, sizeof(block), sizeof(HashTableHeader) + bucketId * sizeof(Bucket), option);

            for (uint32_t inBlockId = 0; inBlockId < HashTable::BLOCK_SIZE; ++inBlockId)
            {
                Bucket& curBucket = block[inBlockId];
                if (curBucket.IsEmpty())
                {
                    continue;
                }
                if (curBucket.IsEqual(key))
                {
                    value = curBucket.Value();
                    return curBucket.IsDeleted() ? misc::DELETED : misc::OK;
                }
            }
        }
        return misc::NOT_FOUND;
    }
    uint64_t Size() const { return mKeyCount; }
    bool IsFull() const { return true; }

protected:
    file_system::SliceFileReaderPtr mHeaderReader;
    uint64_t mBucketCount = 0;
    uint64_t mBlockCount;
    uint64_t mKeyCount = 0;
    uint8_t mNumHashFunc = 2;
    file_system::FileReader* mFileReader = nullptr;

private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE4_CUSTOM(typename, typename, bool, bool, util, CuckooHashTableFileReader);

////////////////////////////////////////////////////////////////////////
// hide some methods
template<typename _KT, typename _VT, bool useCompactBucket>
class CuckooHashTableFileReader<_KT, _VT, true, useCompactBucket> : public CuckooHashTableFileReader<_KT, _VT, false, useCompactBucket>
{
private:
    typedef CuckooHashTableFileReader<_KT, _VT, false, useCompactBucket> Base;
    typedef typename CuckooHashTable<_KT, _VT, true, useCompactBucket>::HashTableHeader HashTableHeader;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;
    using Base::mBucketCount;
    using Base::mFileReader;

public:
    misc::Status Find(
        const _KT& key, _VT& value, file_system::BlockAccessCounter* blockCounter) const
    {
        if (likely(!Bucket::IsEmptyKey(key) && !Bucket::IsDeleteKey(key)))
        {
            return Base::Find(key, value, blockCounter);
        }

        size_t offset = sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket)
                        + (Bucket::IsEmptyKey(key) ? 0 : sizeof(SpecialBucket));
        SpecialBucket bucket;
        file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.advice = storage::IO_ADVICE_LOW_LATENCY;
        mFileReader->Read(&bucket, sizeof(bucket), offset, option);
        if (bucket.IsEmpty())
        {
            return misc::NOT_FOUND;
        }
        value = bucket.Value();
        return bucket.IsDeleted() ? misc::DELETED : misc::OK;
    }
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CUCKOO_HASH_TABLE_FILE_READER_H

#ifndef __INDEXLIB_DENSE_HASH_TABLE_FILE_READER_H
#define __INDEXLIB_DENSE_HASH_TABLE_FILE_READER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/file_system/slice_file.h"
#include "indexlib/file_system/slice_file_reader.h"
#include "indexlib/util/path_util.h"
#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/file_system/read_option.h"
#include "indexlib/file_system/block_file_node.h"

IE_NAMESPACE_BEGIN(common);

// FORMAT: see dense_hash_table.h
template<typename _KT, typename _VT,
         bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
         bool useCompactBucket = false>
class DenseHashTableFileReader
{
public:
    DenseHashTableFileReader() = default;
    ~DenseHashTableFileReader() = default;

private:
    typedef DenseHashTable<_KT, _VT, HasSpecialKey> HashTable;
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
        if (size < sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket))
        {
            IE_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]",
                   size, sizeof(HashTableHeader), mBucketCount * sizeof(Bucket));
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
      mFileNode = DYNAMIC_POINTER_CAST(file_system::BlockFileNode,
                                       fileReader->GetFileNode())
                      .get();
      if (!mFileNode) {
        IE_LOG(ERROR, "key file [%s] is not loaded as block cache",
               fileReader->GetPath().c_str());
        return false;
        }

        return true;
    }

    misc::Status Find(
        const _KT& key, _VT& value, file_system::BlockAccessCounter* blockCounter) const
    {
        uint64_t bucketCount = mBucketCount;
        uint64_t bucketId = key % bucketCount;
        uint64_t probeCount = 0;
        file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.advice = storage::IO_ADVICE_LOW_LATENCY;

        auto accessor = mFileNode->GetAccessor();
        util::BlockHandle handle;
        size_t blockOffset = 0;
        while (true)
        {
            Bucket bucket;
            size_t offset = sizeof(HashTableHeader) + bucketId * sizeof(Bucket);
            if (accessor->GetBlockCount(offset, sizeof(bucket)) == 1)
            {
                if (!handle.GetData() || offset < blockOffset
                    || offset >= blockOffset + handle.GetDataSize())
                {
                    if (offset + sizeof(bucket) > accessor->GetFileLength())
                    {
                        INDEXLIB_FATAL_ERROR(OutOfRange, "read file out of range, offset: [%lu], "
                            "read length: [%lu], file length: [%lu]",
                            offset, sizeof(bucket), accessor->GetFileLength());
                    }
                    if (unlikely(!accessor->GetBlock(offset, handle, &option)))
                    {
                        INDEXLIB_FATAL_ERROR(InconsistentState, "Get Block Handle at offset [%lu] "
                                  "of file length[%lu] failed", offset, accessor->GetFileLength());
                    }
                }
                blockOffset = handle.GetOffset();                
                ::memcpy((void*)(&bucket), (void*)(handle.GetData() + offset - blockOffset), sizeof(bucket));
            }
            else
            {
                mFileNode->Read(&bucket, sizeof(bucket),
                    sizeof(HashTableHeader) + bucketId * sizeof(Bucket), option);
            }
            if (bucket.IsEmpty()) // not found
            {
                return misc::NOT_FOUND;
            }
            else if (bucket.IsEqual(key)) // found it or deleted
            {
                value = bucket.Value();
                return bucket.IsDeleted() ? misc::DELETED : misc::OK;
            }
            if (unlikely(!HashTable::Probe(key, probeCount, bucketId, bucketCount)))
            {
                break;
            }
        }
        return misc::NOT_FOUND;
    }
    uint64_t Size() const { return mKeyCount; }
    bool IsFull() const { return true; }

protected:
    file_system::SliceFileReaderPtr mHeaderReader;
    uint64_t mBucketCount = 0;
    uint64_t mKeyCount = 0;
    file_system::BlockFileNode* mFileNode = nullptr;

private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE4_CUSTOM(typename, typename, bool, bool, util, DenseHashTableFileReader);

////////////////////////////////////////////////////////////////////////
// hide some methods
template<typename _KT, typename _VT, bool useCompactBucket>
class DenseHashTableFileReader<_KT, _VT, true, useCompactBucket> : public DenseHashTableFileReader<_KT, _VT, false, useCompactBucket>
{
private:
    typedef DenseHashTableFileReader<_KT, _VT, false, useCompactBucket> Base;
    typedef typename DenseHashTable<_KT, _VT, true, useCompactBucket>::HashTableHeader HashTableHeader;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;
    using Base::mBucketCount;
    using Base::mFileNode;

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
        mFileNode->Read(&bucket, sizeof(bucket), offset, option);
        if (bucket.IsEmpty())
        {
            return misc::NOT_FOUND;
        }
        value = bucket.Value();
        return bucket.IsDeleted() ? misc::DELETED : misc::OK;
    }
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DENSE_HASH_TABLE_FILE_READER_H

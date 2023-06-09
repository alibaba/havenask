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
#ifndef __INDEXLIB_DENSE_HASH_TABLE_FILE_READER_H
#define __INDEXLIB_DENSE_HASH_TABLE_FILE_READER_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/Try.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/common/hash_table/dense_hash_table.h"
#include "indexlib/common/hash_table/hash_table_file_reader_base.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace common {

// FORMAT: see dense_hash_table.h
template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class DenseHashTableFileReaderBase : public HashTableFileReaderBase
{
public:
    DenseHashTableFileReaderBase() = default;
    ~DenseHashTableFileReaderBase() = default;

public:
    typedef DenseHashTableBase<_KT, _VT, HasSpecialKey> HashTable;
    typedef typename HashTable::HashTableHeader HashTableHeader;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;

private:
    bool ReadHeader(const file_system::DirectoryPtr& directory, const file_system::FileReaderPtr& fileReader)
    {
        size_t size = fileReader->GetLogicLength();
        HashTableHeader header;
        if (size < sizeof(header)) {
            IE_LOG(ERROR, "read header failed, header size[%lu], fileLength[%lu]", sizeof(header),
                   fileReader->GetLogicLength());
            return false;
        }
        std::string fileName;
        util::PathUtil::GetRelativePath(directory->GetLogicalPath(), fileReader->GetLogicalPath(), fileName);
        std::string sliceFileName = fileName + ".header";
        mHeaderReader = directory->CreateFileReader(sliceFileName, file_system::FSOT_SLICE);
        if (!mHeaderReader) {
            size_t sliceLen = sizeof(header);
            auto fileWriter = directory->CreateFileWriter(sliceFileName, file_system::WriterOption::Slice(sliceLen, 1));
            auto status = fileWriter->Truncate(sliceLen).Status();
            if (!status.IsOK()) {
                IE_LOG(ERROR, "truncate header failed: %s", status.ToString().c_str());
                return false;
            }
            mHeaderReader = directory->CreateFileReader(sliceFileName, file_system::FSOT_SLICE);
            status = fileWriter->Close().Status();
            if (!status.IsOK()) {
                IE_LOG(ERROR, "close file writer failed: %s", status.ToString().c_str());
                return false;
            }
            auto [readStatus, readedBytes] =
                fileReader->Read(mHeaderReader->GetBaseAddress(), sliceLen, 0).StatusWith();
            if (!readStatus.IsOK() || readedBytes != sliceLen) {
                IE_LOG(ERROR, "read header failed, status[%s], header size[%lu], readed size[%lu], fileLength[%lu]",
                       status.ToString().c_str(), sizeof(header), readedBytes, fileReader->GetLogicLength());
                return false;
            }
        }
        header = *(HashTableHeader*)mHeaderReader->GetBaseAddress();
        mBucketCount = header.bucketCount;
        mKeyCount = header.keyCount;
        if (size < sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket)) {
            IE_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]", size, sizeof(HashTableHeader),
                   mBucketCount * sizeof(Bucket));
            return false;
        }
        mFileNode = DYNAMIC_POINTER_CAST(file_system::BlockFileNode, fileReader->GetFileNode()).get();
        if (!mFileNode) {
            IE_LOG(ERROR, "key file [%s] is not loaded as block cache", fileReader->DebugString().c_str());
            return false;
        }

        return true;
    }

public:
    bool Init(const file_system::DirectoryPtr& directory, const file_system::FileReaderPtr& fileReader)
    {
        if (!ReadHeader(directory, fileReader)) {
            return false;
        }
        mFileNode = DYNAMIC_POINTER_CAST(file_system::BlockFileNode, fileReader->GetFileNode()).get();
        if (!mFileNode) {
            IE_LOG(ERROR, "key file [%s] is not loaded as block cache", fileReader->DebugString().c_str());
            return false;
        }

        return true;
    }

    using HashTableFileReaderBase::Find;
    FL_LAZY(util::Status)
    Find(const _KT& key, _VT& value, util::BlockAccessCounter* blockCounter,
         autil::TimeoutTerminator* timeoutTerminator) const
    {
        uint64_t bucketCount = mBucketCount;
        uint64_t bucketId = key % bucketCount;
        uint64_t probeCount = 0;

        file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.timeoutTerminator = timeoutTerminator;
        option.advice = file_system::IO_ADVICE_LOW_LATENCY;

        auto accessor = mFileNode->GetAccessor();
        util::BlockHandle handle;
        size_t blockOffset = 0;
        while (true) {
            Bucket bucket;
            size_t offset = sizeof(HashTableHeader) + bucketId * sizeof(Bucket);
            if (accessor->GetBlockCount(offset, sizeof(bucket)) == 1) {
                if (!handle.GetData() || offset < blockOffset || offset >= blockOffset + handle.GetDataSize()) {
                    if (offset + sizeof(bucket) > accessor->GetFileLength()) {
                        INDEXLIB_FATAL_ERROR(OutOfRange,
                                             "read file out of range, offset: [%lu], "
                                             "read length: [%lu], file length: [%lu]",
                                             offset, sizeof(bucket), accessor->GetFileLength());
                    }
                    handle = (FL_COAWAIT accessor->GetBlockAsyncCoro(offset, option)).GetOrThrow();
                }
                blockOffset = handle.GetOffset();
                ::memcpy((void*)&bucket, handle.GetData() + offset - blockOffset, sizeof(bucket));
            } else {
                auto result =
                    (FL_COAWAIT mFileNode->ReadAsyncCoro(&bucket, sizeof(bucket), offset, option)).GetOrThrow();
                if (unlikely(result != sizeof(Bucket))) {
                    INDEXLIB_FATAL_ERROR(IndexCollapsed,
                                         "fileReader read length [%lu]"
                                         "not same as expected [%lu]",
                                         result, sizeof(Bucket));
                    FL_CORETURN util::NOT_FOUND;
                }
            }
            if (bucket.IsEmpty()) // not found
            {
                FL_CORETURN util::NOT_FOUND;
            } else if (bucket.IsEqual(key)) // found it or deleted
            {
                value = bucket.Value();
                FL_CORETURN bucket.IsDeleted() ? util::DELETED : util::OK;
            }
            if (unlikely(!HashTable::Probe(key, probeCount, bucketId, bucketCount))) {
                break;
            }
        }
        FL_CORETURN util::NOT_FOUND;
    }

    uint64_t Size() const { return mKeyCount; }
    bool IsFull() const { return true; }

protected:
    file_system::FileReaderPtr mHeaderReader;
    uint64_t mBucketCount = 0;
    uint64_t mKeyCount = 0;
    file_system::BlockFileNode* mFileNode = nullptr;

private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE4_CUSTOM(typename, typename, bool, bool, util, DenseHashTableFileReaderBase);

////////////////////////////////////////////////////////////////////////
// hide some methods
template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class DenseHashTableFileReader : public DenseHashTableFileReaderBase<_KT, _VT, HasSpecialKey, useCompactBucket>
{
private:
    typedef DenseHashTableFileReaderBase<_KT, _VT, HasSpecialKey, useCompactBucket> Base;

public:
    using Base::Find;
    bool Init(const file_system::DirectoryPtr& directory, const file_system::FileReaderPtr& fileReader) override
    {
        return Base::Init(directory, fileReader);
    }
    FL_LAZY(util::Status)
    Find(uint64_t key, autil::StringView& value, util::BlockAccessCounter* blockCounter, autil::mem_pool::Pool* pool,
         autil::TimeoutTerminator* timeoutTerminator) const override final
    {
        assert(pool);
        _VT* typedValue = (_VT*)pool->allocate(sizeof(_VT));
        auto status = FL_COAWAIT Find((_KT)key, *typedValue, blockCounter, timeoutTerminator);
        value = {(char*)typedValue, sizeof(_VT)};
        FL_CORETURN status;
    }
};

template <typename _KT, typename _VT, bool useCompactBucket>
class DenseHashTableFileReader<_KT, _VT, true, useCompactBucket>
    : public DenseHashTableFileReaderBase<_KT, _VT, false, useCompactBucket>
{
public:
    typedef typename DenseHashTable<_KT, _VT, true, useCompactBucket>::HashTableHeader HashTableHeader;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;

private:
    typedef DenseHashTableFileReaderBase<_KT, _VT, false, useCompactBucket> Base;
    using Base::mBucketCount;
    using Base::mFileNode;

public:
    using Base::Find;
    bool Init(const file_system::DirectoryPtr& directory, const file_system::FileReaderPtr& fileReader) override
    {
        return Base::Init(directory, fileReader);
    }
    FL_LAZY(util::Status)
    Find(uint64_t key, autil::StringView& value, util::BlockAccessCounter* blockCounter, autil::mem_pool::Pool* pool,
         autil::TimeoutTerminator* timeoutTerminator) const override final
    {
        assert(pool);
        _VT* typedValue = (_VT*)pool->allocate(sizeof(_VT));
        auto status = FL_COAWAIT Find((_KT)key, *typedValue, blockCounter, timeoutTerminator);
        value = {(char*)typedValue, sizeof(_VT)};
        FL_CORETURN status;
    }

    FL_LAZY(util::Status)
    Find(const _KT& key, _VT& value, util::BlockAccessCounter* blockCounter,
         autil::TimeoutTerminator* timeoutTerminator) const
    {
        if (likely(!Bucket::IsEmptyKey(key) && !Bucket::IsDeleteKey(key))) {
            FL_CORETURN FL_COAWAIT Base::Find(key, value, blockCounter, timeoutTerminator);
        }

        size_t offset = sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket) +
                        (Bucket::IsEmptyKey(key) ? 0 : sizeof(SpecialBucket));
        SpecialBucket bucket;
        file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.timeoutTerminator = timeoutTerminator;
        option.advice = file_system::IO_ADVICE_LOW_LATENCY;
        auto result = FL_COAWAIT mFileNode->ReadAsyncCoro(&bucket, sizeof(bucket), offset, option);
        if (!result.OK()) {
            FL_CORETURN util::FAIL;
        }
        if (bucket.IsEmpty()) {
            FL_CORETURN util::NOT_FOUND;
        }
        value = bucket.Value();
        FL_CORETURN bucket.IsDeleted() ? util::DELETED : util::OK;
    }
};
}} // namespace indexlib::common

#endif //__INDEXLIB_DENSE_HASH_TABLE_FILE_READER_H

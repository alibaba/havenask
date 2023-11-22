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

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/hash_table/cuckoo_hash_table.h"
#include "indexlib/common/hash_table/hash_table_file_reader_base.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace common {

// FORMAT: see cuckoo_hash_table.h
template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class CuckooHashTableFileReaderBase : public HashTableFileReaderBase
{
public:
    CuckooHashTableFileReaderBase() = default;
    ~CuckooHashTableFileReaderBase() = default;

public:
    typedef CuckooHashTable<_KT, _VT, HasSpecialKey, useCompactBucket> HashTable;
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
            fileWriter->Truncate(sliceLen).GetOrThrow();
            mHeaderReader = directory->CreateFileReader(sliceFileName, file_system::FSOT_SLICE);
            fileWriter->Close().GetOrThrow();
            if (sliceLen != fileReader->Read(mHeaderReader->GetBaseAddress(), sliceLen, 0).GetOrThrow()) {
                IE_LOG(ERROR, "read header failed, header size[%lu], fileLength[%lu]", sizeof(header),
                       fileReader->GetLogicLength());
                return false;
            }
        }
        header = *(HashTableHeader*)mHeaderReader->GetBaseAddress();
        mBucketCount = header.bucketCount;
        mKeyCount = header.keyCount;
        mNumHashFunc = header.numHashFunc;
        if (size < sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket)) {
            IE_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]", size, sizeof(HashTableHeader),
                   mBucketCount * sizeof(Bucket));
            return false;
        }
        mBlockCount = (size - sizeof(HashTableHeader) - (HasSpecialKey ? sizeof(Bucket) * 2 : 0)) / sizeof(Bucket) /
                      HashTable::BLOCK_SIZE;
        if (mBlockCount < 1) {
            IE_LOG(ERROR, "size[%lu] too small", size);
            return false;
        }
        mFileReader = fileReader.get();
        return true;
    }

public:
    bool Init(const file_system::DirectoryPtr& directory, const file_system::FileReaderPtr& fileReader)
    {
        if (!ReadHeader(directory, fileReader)) {
            return false;
        }
        mFileReader = fileReader.get();
        return true;
    }

    using HashTableFileReaderBase::Find;
    FL_LAZY(util::Status)
    Find(const _KT& key, _VT& value, util::BlockAccessCounter* blockCounter,
         autil::TimeoutTerminator* timeoutTerminator) const
    {
        file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.timeoutTerminator = timeoutTerminator;
        option.advice = file_system::IO_ADVICE_LOW_LATENCY;
        for (uint32_t hashCnt = 0; hashCnt < mNumHashFunc; ++hashCnt) {
            const _KT& hash = HashTable::CuckooHash(key, hashCnt);
            uint64_t bucketId = HashTable::GetFirstBucketIdInBlock(hash, mBlockCount);
            Bucket block[HashTable::BLOCK_SIZE];
            auto result = (FL_COAWAIT mFileReader->ReadAsyncCoro(
                               block, sizeof(block), sizeof(HashTableHeader) + bucketId * sizeof(Bucket), option))
                              .GetOrThrow();
            (void)result;

            for (uint32_t inBlockId = 0; inBlockId < HashTable::BLOCK_SIZE; ++inBlockId) {
                Bucket& curBucket = block[inBlockId];
                if (curBucket.IsEmpty()) {
                    continue;
                }
                if (curBucket.IsEqual(key)) {
                    value = curBucket.Value();
                    FL_CORETURN curBucket.IsDeleted() ? util::DELETED : util::OK;
                }
            }
        }
        FL_CORETURN util::NOT_FOUND;
    }
    uint64_t Size() const { return mKeyCount; }
    bool IsFull() const { return true; }

protected:
    file_system::FileReaderPtr mHeaderReader;
    uint64_t mBucketCount = 0;
    uint64_t mBlockCount;
    uint64_t mKeyCount = 0;
    uint8_t mNumHashFunc = 2;
    file_system::FileReader* mFileReader = nullptr;

private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE4_CUSTOM(typename, typename, bool, bool, util, CuckooHashTableFileReaderBase);

////////////////////////////////////////////////////////////////////////
// hide some methods
template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class CuckooHashTableFileReader : public CuckooHashTableFileReaderBase<_KT, _VT, HasSpecialKey, useCompactBucket>
{
public:
    typedef CuckooHashTableFileReaderBase<_KT, _VT, HasSpecialKey, useCompactBucket> Base;

public:
    bool Init(const file_system::DirectoryPtr& directory, const file_system::FileReaderPtr& fileReader) override
    {
        return Base::Init(directory, fileReader);
    }
    using Base::Find;
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
class CuckooHashTableFileReader<_KT, _VT, true, useCompactBucket>
    : public CuckooHashTableFileReaderBase<_KT, _VT, false, useCompactBucket>
{
public:
    typedef typename CuckooHashTableBase<_KT, _VT, true, useCompactBucket>::HashTableHeader HashTableHeader;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;

private:
    typedef CuckooHashTableFileReaderBase<_KT, _VT, false, useCompactBucket> Base;
    using Base::mBucketCount;
    using Base::mFileReader;

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
        auto result = FL_COAWAIT mFileReader->ReadAsyncCoro(&bucket, sizeof(bucket), offset, option);
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

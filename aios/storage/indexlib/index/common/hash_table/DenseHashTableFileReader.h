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
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/Try.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/common/hash_table/ClosedHashTableTraits.h"
#include "indexlib/index/common/hash_table/DenseHashTable.h"
#include "indexlib/index/common/hash_table/HashTableFileReaderBase.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {

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
    bool ReadHeader(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                    const indexlib::file_system::FileReaderPtr& fileReader)
    {
        size_t size = fileReader->GetLogicLength();
        HashTableHeader header;
        if (size < sizeof(header)) {
            AUTIL_LOG(ERROR, "read header failed, header size[%lu], fileLength[%lu]", sizeof(header),
                      fileReader->GetLogicLength());
            return false;
        }
        std::string fileName;
        indexlib::util::PathUtil::GetRelativePath(directory->GetLogicalPath(), fileReader->GetLogicalPath(), fileName);
        std::string sliceFileName = fileName + ".header";
        _headerReader = directory->CreateFileReader(sliceFileName, indexlib::file_system::FSOT_SLICE).GetOrThrow();
        if (!_headerReader) {
            size_t sliceLen = sizeof(header);
            auto fileWriter =
                directory->CreateFileWriter(sliceFileName, indexlib::file_system::WriterOption::Slice(sliceLen, 1))
                    .GetOrThrow();
            auto status = fileWriter->Truncate(sliceLen).Status();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "truncate header failed: %s", status.ToString().c_str());
                return false;
            }
            _headerReader = directory->CreateFileReader(sliceFileName, indexlib::file_system::FSOT_SLICE).GetOrThrow();
            status = fileWriter->Close().Status();
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "close file writer failed: %s", status.ToString().c_str());
                return false;
            }
            auto [readStatus, readedBytes] =
                fileReader->Read(_headerReader->GetBaseAddress(), sliceLen, 0).StatusWith();
            if (!readStatus.IsOK() || readedBytes != sliceLen) {
                AUTIL_LOG(ERROR, "read header failed, status[%s], header size[%lu], readed size[%lu], fileLength[%lu]",
                          status.ToString().c_str(), sizeof(header), readedBytes, fileReader->GetLogicLength());
                return false;
            }
        }
        header = *(HashTableHeader*)_headerReader->GetBaseAddress();
        _bucketCount = header.bucketCount;
        _keyCount = header.keyCount;
        if (size < sizeof(HashTableHeader) + _bucketCount * sizeof(Bucket)) {
            AUTIL_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]", size, sizeof(HashTableHeader),
                      _bucketCount * sizeof(Bucket));
            return false;
        }
        _fileNode = std::dynamic_pointer_cast<indexlib::file_system::BlockFileNode>(fileReader->GetFileNode()).get();
        if (!_fileNode) {
            AUTIL_LOG(ERROR, "key file [%s] is not loaded as block cache", fileReader->DebugString().c_str());
            return false;
        }

        return true;
    }

public:
    bool Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
              const indexlib::file_system::FileReaderPtr& fileReader) override
    {
        if (!ReadHeader(directory, fileReader)) {
            return false;
        }
        _fileNode = std::dynamic_pointer_cast<indexlib::file_system::BlockFileNode>(fileReader->GetFileNode()).get();
        if (!_fileNode) {
            AUTIL_LOG(ERROR, "key file [%s] is not loaded as block cache", fileReader->DebugString().c_str());
            return false;
        }

        return true;
    }

    using HashTableFileReaderBase::Find;
    FL_LAZY(indexlib::util::Status)
    Find(const _KT& key, _VT& value, indexlib::util::BlockAccessCounter* blockCounter,
         autil::TimeoutTerminator* timeoutTerminator) const
    {
        uint64_t bucketCount = _bucketCount;
        uint64_t bucketId = key % bucketCount;
        uint64_t probeCount = 0;

        indexlib::file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.timeoutTerminator = timeoutTerminator;
        option.advice = indexlib::file_system::IO_ADVICE_LOW_LATENCY;

        auto accessor = _fileNode->GetAccessor();
        indexlib::util::BlockHandle handle;
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
                    (FL_COAWAIT _fileNode->ReadAsyncCoro(&bucket, sizeof(bucket), offset, option)).GetOrThrow();
                if (unlikely(result != sizeof(Bucket))) {
                    INDEXLIB_FATAL_ERROR(IndexCollapsed,
                                         "fileReader read length [%lu]"
                                         "not same as expected [%lu]",
                                         result, sizeof(Bucket));
                    FL_CORETURN indexlib::util::NOT_FOUND;
                }
            }
            if (bucket.IsEmpty()) // not found
            {
                FL_CORETURN indexlib::util::NOT_FOUND;
            } else if (bucket.IsEqual(key)) // found it or deleted
            {
                value = bucket.Value();
                FL_CORETURN bucket.IsDeleted() ? indexlib::util::DELETED : indexlib::util::OK;
            }
            if (unlikely(!HashTable::Probe(key, probeCount, bucketId, bucketCount))) {
                break;
            }
        }
        FL_CORETURN indexlib::util::NOT_FOUND;
    }

    uint64_t Size() const { return _keyCount; }
    bool IsFull() const { return true; }

protected:
    indexlib::file_system::FileReaderPtr _headerReader;
    uint64_t _bucketCount = 0;
    uint64_t _keyCount = 0;
    indexlib::file_system::BlockFileNode* _fileNode = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
alog::Logger* DenseHashTableFileReaderBase<_KT, _VT, HasSpecialKey, useCompactBucket>::_logger =
    alog::Logger::getLogger("indexlib.index.DenseHashTableFileReaderBase");

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
    bool Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
              const indexlib::file_system::FileReaderPtr& fileReader) override
    {
        return Base::Init(directory, fileReader);
    }
    FL_LAZY(indexlib::util::Status)
    Find(uint64_t key, autil::StringView& value, indexlib::util::BlockAccessCounter* blockCounter,
         autil::mem_pool::Pool* pool, autil::TimeoutTerminator* timeoutTerminator) const override final
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
    using Base::_bucketCount;
    using Base::_fileNode;

public:
    using Base::Find;
    bool Init(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
              const indexlib::file_system::FileReaderPtr& fileReader) override
    {
        return Base::Init(directory, fileReader);
    }
    FL_LAZY(indexlib::util::Status)
    Find(uint64_t key, autil::StringView& value, indexlib::util::BlockAccessCounter* blockCounter,
         autil::mem_pool::Pool* pool, autil::TimeoutTerminator* timeoutTerminator) const override final
    {
        assert(pool);
        _VT* typedValue = (_VT*)pool->allocate(sizeof(_VT));
        auto status = FL_COAWAIT Find((_KT)key, *typedValue, blockCounter, timeoutTerminator);
        value = {(char*)typedValue, sizeof(_VT)};
        FL_CORETURN status;
    }

    FL_LAZY(indexlib::util::Status)
    Find(const _KT& key, _VT& value, indexlib::util::BlockAccessCounter* blockCounter,
         autil::TimeoutTerminator* timeoutTerminator) const
    {
        if (likely(!Bucket::IsEmptyKey(key) && !Bucket::IsDeleteKey(key))) {
            FL_CORETURN FL_COAWAIT Base::Find(key, value, blockCounter, timeoutTerminator);
        }

        size_t offset = sizeof(HashTableHeader) + _bucketCount * sizeof(Bucket) +
                        (Bucket::IsEmptyKey(key) ? 0 : sizeof(SpecialBucket));
        SpecialBucket bucket;
        indexlib::file_system::ReadOption option;
        option.blockCounter = blockCounter;
        option.timeoutTerminator = timeoutTerminator;
        option.advice = indexlib::file_system::IO_ADVICE_LOW_LATENCY;
        auto result = FL_COAWAIT _fileNode->ReadAsyncCoro(&bucket, sizeof(bucket), offset, option);
        if (!result.OK()) {
            FL_CORETURN indexlib::util::FAIL;
        }
        if (bucket.IsEmpty()) {
            FL_CORETURN indexlib::util::NOT_FOUND;
        }
        value = bucket.Value();
        FL_CORETURN bucket.IsDeleted() ? indexlib::util::DELETED : indexlib::util::OK;
    }
};
} // namespace indexlibv2::index

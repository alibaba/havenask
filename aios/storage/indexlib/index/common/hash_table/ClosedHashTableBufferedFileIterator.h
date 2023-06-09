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
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/hash_table/ClosedHashTableTraits.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"

namespace indexlibv2::index {

// FORMAT: see dense_hash_table.h or cuckoo_hash_table.h
template <typename HashTableHeader, typename _KT, typename _VT,
          bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey, bool useCompactBucket = false>
class ClosedHashTableBufferedFileIterator : public HashTableFileIterator
{
protected:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    struct KVTuple {
        _KT key;
        _VT value;
        bool deleted;
    };

public:
    size_t EstimateMemoryUse(size_t keyCount) const override final { return 0; }

public:
    bool Init(const indexlib::file_system::FileReaderPtr& fileReader) override
    {
        return DoInit(fileReader, fileReader->GetLogicLength());
    }
    size_t Size() const override { return _keyCount; }
    bool IsValid() const override { return _isValid; }
    void MoveToNext() override
    {
        assert(IsValid());
        DoMoveToNext<Bucket>(_length);
    }
    bool IsDeleted() const override
    {
        assert(IsValid());
        return _kVTuple.deleted;
    }
    uint64_t GetKey() const override final { return Key(); }
    autil::StringView GetValue() const override final
    {
        assert(IsValid());
        return autil::StringView((const char*)std::addressof(_kVTuple.value), sizeof(_kVTuple.value));
    }
    void SortByKey() override final { assert(false); }
    void SortByValue() override final { assert(false); }
    bool Seek(int64_t offset) override
    {
        if (offset >= _length) {
            return false;
        }
        _offset = offset;
        if (0 == offset) {
            Reset();
        }
        return true;
    }
    void Reset() override
    {
        _isValid = true;
        _offset = sizeof(HashTableHeader);
        MoveToNext();
    }
    virtual int64_t GetOffset() const override final { return _offset; }

public:
    const _KT& Key() const
    {
        assert(IsValid());
        return _kVTuple.key;
    }
    const _VT& Value() const
    {
        assert(IsValid());
        return _kVTuple.value;
    }

protected:
    bool DoInit(const indexlib::file_system::FileReaderPtr& fileReader, size_t length)
    {
        _fileReader = fileReader;
        _length = length;
        HashTableHeader header;
        if (sizeof(header) != fileReader->Read(&header, sizeof(header), 0).GetOrThrow()) {
            AUTIL_LOG(ERROR,
                      "read header failed from file[%s], "
                      "headerSize[%lu], fileLength[%lu]",
                      fileReader->DebugString().c_str(), sizeof(header), fileReader->GetLogicLength());
            return false;
        }
        _keyCount = header.keyCount;
        size_t bucketCount = header.bucketCount;
        if (sizeof(Bucket) * bucketCount + sizeof(header) != _length) {
            AUTIL_LOG(ERROR, "invalid file[%s], size[%lu], headerSize[%lu], length[%lu]",
                      fileReader->DebugString().c_str(), fileReader->GetLogicLength(), sizeof(header), length);
            return false;
        }
        Reset();
        return true;
    }
    template <typename Bucket>
    void DoMoveToNext(size_t endOffset)
    {
        Bucket bucket;
        while (bucket.IsEmpty()) {
            if (_offset >= endOffset) {
                _isValid = false;
                return;
            }
            if (sizeof(bucket) != _fileReader->Read(&bucket, sizeof(bucket), _offset).GetOrThrow()) {
                AUTIL_LOG(ERROR, "read bucket failed from file[%s], offset[%lu]", _fileReader->DebugString().c_str(),
                          _offset);
                return;
            }
            _offset += sizeof(Bucket);
        }
        _kVTuple.key = bucket.Key();
        _kVTuple.value = bucket.Value();
        _kVTuple.deleted = bucket.IsDeleted();
    }

protected:
    indexlib::file_system::FileReaderPtr _fileReader;
    size_t _offset = 0;
    size_t _length = 0;
    int64_t _keyCount = 0;
    KVTuple _kVTuple;
    bool _isValid = false;

protected:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////
template <typename HashTableHeader, typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
alog::Logger* ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, HasSpecialKey, useCompactBucket>::_logger =
    alog::Logger::getLogger("indexlib.index.ClosedHashTableBufferedFileIterator");

////////////////////////////////////////////////////////////////////////
// hide some methods
template <typename HashTableHeader, typename _KT, typename _VT, bool useCompactBucket>
class ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, true, useCompactBucket>
    : public ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, false, useCompactBucket>
{
private:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;
    typedef ClosedHashTableBufferedFileIterator<HashTableHeader, _KT, _VT, false, useCompactBucket> Base;
    using Base::_logger;

public:
    bool Init(const indexlib::file_system::FileReaderPtr& fileReader) override
    {
        _fileLength = fileReader->GetLogicLength();
        if (_fileLength <= sizeof(SpecialBucket) * 2 + sizeof(HashTableHeader)) {
            AUTIL_LOG(ERROR, "invalid file size[%lu], headerSize[%lu]", fileReader->GetLogicLength(),
                      sizeof(HashTableHeader));
            return false;
        }
        if (!Base::DoInit(fileReader, _fileLength - sizeof(SpecialBucket) * 2)) {
            return false;
        }
        if (!IsValid()) {
            Base::_isValid = true;
            MoveToNext();
        }
        return true;
    }
    bool IsValid() const override { return Base::_isValid; }
    bool Seek(int64_t offset) override
    {
        if (offset > Base::_length && offset > _fileLength) {
            return false;
        }
        Base::_offset = offset;
        if (0 == offset) {
            Reset();
        }
        return true;
    }
    void Reset() override
    {
        Base::_isValid = true;
        Base::_offset = sizeof(HashTableHeader);
        MoveToNext();
    }
    void MoveToNext() override
    {
        assert(IsValid());
        if (likely(Base::_offset < Base::_length)) {
            Base::template DoMoveToNext<typename Base::Bucket>(Base::_length);
            if (likely(Base::_isValid)) {
                return;
            }
            Base::_isValid = true;
        }
        return Base::template DoMoveToNext<SpecialBucket>(_fileLength);
    }

private:
    size_t _fileLength = 0;
};
} // namespace indexlibv2::index

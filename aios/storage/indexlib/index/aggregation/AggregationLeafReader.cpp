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
#include "indexlib/index/aggregation/AggregationLeafReader.h"

#include <algorithm>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/aggregation/RandomAccessValueIterator.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AggregationLeafReader);

class KeyIteratorImpl final : public IKeyIterator
{
public:
    KeyIteratorImpl(const KeyMeta* begin, size_t count) : _begin(begin), _count(count), _cursor(0) {}

public:
    bool HasNext() const override { return _cursor < _count; }
    uint64_t Next() override
    {
        const KeyMeta& meta = _begin[_cursor++];
        return meta.key;
    }

private:
    const KeyMeta* _begin = nullptr;
    size_t _count = 0;
    size_t _cursor = 0;
};

class InMemoryValueIterator final : public RandomAccessValueIterator
{
public:
    InMemoryValueIterator(const char* baseAddr, size_t valueCount, int32_t valueSize)
        : _baseAddr(baseAddr)
        , _valueCount(valueCount)
        , _valueSize(valueSize)
        , _cursor(0)
    {
    }

public:
    size_t Size() const override { return _valueCount; }
    bool HasNext() const override { return _cursor < _valueCount; }
    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override
    {
        value = Get(_cursor++);
        return Status::OK();
    }
    autil::StringView Get(size_t idx) const override
    {
        assert(idx < _valueCount);
        const char* data = _baseAddr + idx * _valueSize;
        return autil::StringView(data, _valueSize);
    }

private:
    const char* _baseAddr;
    const size_t _valueCount;
    const size_t _valueSize;
    size_t _cursor;
};

class FSValueIterator final : public IValueIterator
{
public:
    FSValueIterator(const std::shared_ptr<indexlib::file_system::FileReader>& valueReader, size_t offset,
                    size_t valueCount, int32_t valueSize)
        : _valueFileReader(valueReader)
        , _offset(offset)
        , _valueCount(valueCount)
        , _valueSize(valueSize)
        , _cursor(0)
    {
    }

public:
    bool HasNext() const override { return _cursor < _valueCount; }
    Status Next(autil::mem_pool::PoolBase* pool, autil::StringView& value) override
    {
        void* data = pool->allocate(_valueSize);
        auto s = _valueFileReader->Read(data, _valueSize, GetOffset(_cursor++)).Status();
        if (!s.IsOK()) {
            return s;
        }
        value = autil::StringView((const char*)data, _valueSize);
        return Status::OK();
    }

private:
    size_t GetOffset(size_t idx) const { return _offset + idx * _valueSize; }

private:
    std::shared_ptr<indexlib::file_system::FileReader> _valueFileReader;
    const size_t _offset;
    const size_t _valueCount;
    const size_t _valueSize;
    size_t _cursor;
};

AggregationLeafReader::AggregationLeafReader() = default;

AggregationLeafReader::~AggregationLeafReader()
{
    if (_ownKeyMemory) {
        delete[] _keyAddr;
    }
}

Status AggregationLeafReader::Open(const std::shared_ptr<indexlibv2::config::AggregationIndexConfig>& indexConfig,
                                   const std::shared_ptr<indexlib::file_system::Directory>& indexRootDir)
{
    const auto& indexName = indexConfig->GetIndexName();

    _valueSize = indexConfig->GetValueConfig()->GetFixedLength();
    if (_valueSize <= 0) {
        return Status::Unimplement("[%s:%s] only support fixed-size value", indexConfig->GetIndexType().c_str(),
                                   indexName.c_str());
    }

    auto indexDir = indexRootDir->GetDirectory(indexName, false);
    if (!indexDir) {
        return Status::Corruption("index dir %s does not exist in %s", indexName.c_str(),
                                  indexRootDir->DebugString().c_str());
    }

    // load index meta
    auto s = OpenMeta(indexDir->GetIDirectory());
    RETURN_IF_STATUS_ERROR(s, "[%s]: load meta failed from %s", indexName.c_str(), indexDir->DebugString().c_str());

    // load key
    s = OpenKey(indexDir->GetIDirectory());
    RETURN_IF_STATUS_ERROR(s, "[%s]: load key failed from %s", indexName.c_str(), indexDir->DebugString().c_str());

    // load value
    s = OpenValue(indexDir->GetIDirectory());
    RETURN_IF_STATUS_ERROR(s, "[%s]: load value failed from %s", indexName.c_str(), indexDir->DebugString().c_str());

    return Status::OK();
}

std::unique_ptr<IKeyIterator> AggregationLeafReader::CreateKeyIterator() const
{
    return std::make_unique<KeyIteratorImpl>(_keyAddr, _indexMeta->keyCount);
}

std::unique_ptr<IValueIterator> AggregationLeafReader::Lookup(uint64_t key, autil::mem_pool::PoolBase* pool) const
{
    const KeyMeta* it = FindKey(key);
    if (it == nullptr) {
        return nullptr;
    }

    const auto& valueMeta = it->valueMeta;
    uint32_t count = valueMeta.valueCountAfterUnique != 0 ? valueMeta.valueCountAfterUnique : valueMeta.valueCount;
    if (_valueAddr != nullptr) {
        return std::make_unique<InMemoryValueIterator>(_valueAddr + it->offset, count, _valueSize);
    } else {
        return std::make_unique<FSValueIterator>(_valueFileReader, it->offset, count, _valueSize);
    }
}

bool AggregationLeafReader::GetValueMeta(uint64_t key, ValueMeta& valueMeta) const
{
    const KeyMeta* keyMeta = FindKey(key);
    if (keyMeta == nullptr) {
        return false;
    }
    valueMeta = keyMeta->valueMeta;
    return true;
}

const KeyMeta* AggregationLeafReader::FindKey(uint64_t key) const
{
    KeyMeta searchKey;
    searchKey.key = key;
    auto it = std::lower_bound(KeyBegin(), KeyEnd(), searchKey);
    if (it == KeyEnd() || it->key != key) {
        // Not Found
        return nullptr;
    }
    return it;
}

Status AggregationLeafReader::OpenMeta(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    std::string content;
    auto s =
        directory->Load(META_FILE_NAME, indexlib::file_system::ReaderOption(indexlib::file_system::FSOT_MEM), content)
            .Status();
    if (!s.IsOK()) {
        return s;
    }
    _indexMeta = std::make_unique<IndexMeta>();
    if (!_indexMeta->FromString(content)) {
        return Status::Corruption("parse index meta from %s failed", content.c_str());
    }
    return Status::OK();
}

Status AggregationLeafReader::OpenKey(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_LOAD_CONFIG);
    auto [s, keyFileReader] = directory->CreateFileReader(KEY_FILE_NAME, readerOption).StatusWith();
    if (!s.IsOK()) {
        return s;
    }

    size_t expectedKeyFileSize = sizeof(KeyMeta) * _indexMeta->keyCount;
    if (expectedKeyFileSize != keyFileReader->GetLogicLength()) {
        return Status::Corruption("key file, expected:%lu, actual:%lu", expectedKeyFileSize,
                                  keyFileReader->GetLogicLength());
    }
    _keyAddr = reinterpret_cast<const KeyMeta*>(keyFileReader->GetBaseAddress());
    if (!_keyAddr) {
        KeyMeta* base = new KeyMeta[_indexMeta->keyCount];
        s = keyFileReader->Read((void*)base, expectedKeyFileSize, 0).Status();
        if (!s.IsOK()) {
            delete[] base;
            return s;
        }
        _keyAddr = base;
        _ownKeyMemory = true;
    } else {
        _keyFileReader = keyFileReader;
    }
    return Status::OK();
}

Status AggregationLeafReader::OpenValue(const std::shared_ptr<indexlib::file_system::IDirectory>& directory)
{
    indexlib::file_system::ReaderOption readerOption(indexlib::file_system::FSOT_LOAD_CONFIG);
    auto [s, valueFileReader] = directory->CreateFileReader(VALUE_FILE_NAME, readerOption).StatusWith();
    if (!s.IsOK()) {
        return s;
    }

    // TODO: remove this check when compression is supported
    size_t expectedSize = _indexMeta->accmulatedValueBytes;
    if (_indexMeta->accmulatedValueBytesAfterUnique > 0) {
        expectedSize = _indexMeta->accmulatedValueBytesAfterUnique;
    }
    if (expectedSize != valueFileReader->GetLogicLength()) {
        return Status::Corruption("value file: expected: %lu, actual: %lu", _indexMeta->accmulatedValueBytes,
                                  valueFileReader->GetLogicLength());
    }
    _valueFileReader = valueFileReader;
    _valueAddr = reinterpret_cast<const char*>(_valueFileReader->GetBaseAddress());
    return Status::OK();
}

} // namespace indexlibv2::index

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

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/hash_table/HashTableNode.h"

namespace indexlibv2::index {

template <typename Key, typename Value>
class HashTableWriter
{
public:
    HashTableWriter() : _nodeCount(0) {};
    ~HashTableWriter() {};

    void Init(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir, const std::string& filePath,
              size_t bucketCount);
    const indexlib::file_system::FileWriterPtr& GetFileWriter() const { return _fileWriter; };

    void Add(const Key& key, const Value& value);
    void Close();

    static size_t GetDumpSize(size_t bucketCount, size_t ite_count)
    {
        return sizeof(HashTableNode<Key, Value>) * ite_count // ite_size
               + sizeof(uint32_t) * bucketCount              // bucketSize
               + sizeof(uint32_t) * 2;                       // sizeof : nodeCount + bucketCount
    }

    bool Empty() const { return _nodeCount == 0; }

    size_t Size() const { return _nodeCount; }

private:
    uint32_t _nodeCount;
    indexlib::file_system::FileWriterPtr _fileWriter;

    std::vector<uint32_t> _bucket;
};

template <typename Key, typename Value>
void HashTableWriter<Key, Value>::Init(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                       const std::string& filePath, size_t bucketCount)
{
    _fileWriter = rootDir->CreateFileWriter(filePath, indexlib::file_system::WriterOption()).GetOrThrow();
    _bucket.resize(bucketCount, INVALID_HASHTABLE_OFFSET);
}

template <typename Key, typename Value>
void HashTableWriter<Key, Value>::Add(const Key& key, const Value& value)
{
    assert(_fileWriter);
    size_t bucketIdx = key % _bucket.size();
    HashTableNode<Key, Value> node(key, value, _bucket[bucketIdx]);
    _fileWriter->Write(&node, sizeof(node)).GetOrThrow();
    _bucket[bucketIdx] = _nodeCount++;
}

template <typename Key, typename Value>
void HashTableWriter<Key, Value>::Close()
{
    assert(_fileWriter);
    _fileWriter->Write(_bucket.data(), _bucket.size() * sizeof(uint32_t)).GetOrThrow();
    _fileWriter->Write(&_nodeCount, sizeof(_nodeCount)).GetOrThrow();
    uint32_t bucketCount = _bucket.size();
    _fileWriter->Write(&bucketCount, sizeof(bucketCount)).GetOrThrow();
    _fileWriter->Close().GetOrThrow();
    _fileWriter.reset();
}
} // namespace indexlibv2::index

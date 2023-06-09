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
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/common/hash_table/HashTableNode.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::index {

template <typename Key, typename Value>
class HashTableReader
{
public:
    HashTableReader() : _nodeCount(0), _bucketCount(0), _bucketBase(NULL), _nodeBase(NULL) {};

    ~HashTableReader() {};

public:
    void Open(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir, const std::string& filePath);
    bool Find(const Key& key, Value& value);
    const Value* Find(const Key& key) const;

    uint32_t GetNodeCount() const { return _nodeCount; }
    uint32_t GetBucketCount() const { return _bucketCount; }
    size_t GetFileLength() const
    {
        assert(_fileReader);
        return _fileReader->GetLogicLength();
    }
    HashTableNode<Key, Value> GetNode(size_t idx) { return _nodeBase[idx]; }

    static void DecodeMeta(const indexlib::file_system::FileReaderPtr& fileReader, uint32_t& nodeSize,
                           uint32_t& bucketCount);

private:
    typedef HashTableNode<Key, Value> NodeType;
    uint32_t _nodeCount;
    uint32_t _bucketCount;
    uint32_t* _bucketBase;
    NodeType* _nodeBase;

    indexlib::file_system::FileReaderPtr _fileReader;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, HashTableReader, Key, Value);

template <typename Key, typename Value>
void HashTableReader<Key, Value>::Open(const std::shared_ptr<indexlib::file_system::IDirectory>& rootDir,
                                       const std::string& filePath)
{
    _fileReader = rootDir->CreateFileReader(filePath, indexlib::file_system::FSOT_MEM_ACCESS).GetOrThrow();
    DecodeMeta(_fileReader, _nodeCount, _bucketCount);
    _nodeBase = static_cast<NodeType*>(_fileReader->GetBaseAddress());

    if (_nodeBase == NULL) {
        INDEXLIB_FATAL_ERROR(Runtime, "HashTableReader [%s] GetBaseAddress failed", _fileReader->DebugString().c_str());
    }
    _bucketBase = reinterpret_cast<uint32_t*>(_nodeBase + _nodeCount);
}

template <typename Key, typename Value>
bool HashTableReader<Key, Value>::Find(const Key& key, Value& value)
{
    uint32_t bucketIdx = key % _bucketCount;
    uint32_t offset = _bucketBase[bucketIdx];
    while (offset != INVALID_HASHTABLE_OFFSET) {
        const NodeType& node = _nodeBase[offset];
        if (node.key == key) {
            value = node.value;
            return true;
        }
        offset = node.next;
    }
    return false;
}

template <typename Key, typename Value>
const Value* HashTableReader<Key, Value>::Find(const Key& key) const
{
    uint32_t bucketIdx = key % _bucketCount;
    uint32_t offset = _bucketBase[bucketIdx];
    while (offset != INVALID_HASHTABLE_OFFSET) {
        const NodeType& node = _nodeBase[offset];
        if (node.key == key) {
            return &(node.value);
        }
        offset = node.next;
    }
    return NULL;
}

template <typename Key, typename Value>
inline void HashTableReader<Key, Value>::DecodeMeta(const indexlib::file_system::FileReaderPtr& fileReader,
                                                    uint32_t& nodeSize, uint32_t& bucketCount)
{
    assert(fileReader);
    size_t fileLength = fileReader->GetLogicLength();
    if (fileLength < sizeof(uint32_t) + sizeof(uint32_t)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "HashTableReader Open Failed,"
                             " file[%s] length[%lu] is smaller than [%lu]",
                             fileReader->DebugString().c_str(), fileLength, sizeof(uint32_t) + sizeof(uint32_t));
    }

    size_t cursor = fileLength - sizeof(uint32_t) - sizeof(uint32_t);
    fileReader->Read(&nodeSize, sizeof(uint32_t), cursor).GetOrThrow();
    cursor += sizeof(uint32_t);
    fileReader->Read(&bucketCount, sizeof(uint32_t), cursor).GetOrThrow();

    size_t totalLength =
        nodeSize * sizeof(NodeType) + bucketCount * sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);
    if (totalLength != fileLength) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "HashTableReader Open Failed,"
                             " file[%s] length[%lu] error, expect length[%lu]",
                             fileReader->DebugString().c_str(), fileLength, totalLength);
    }
}
} // namespace indexlibv2::index

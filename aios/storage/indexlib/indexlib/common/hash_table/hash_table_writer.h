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
#ifndef __INDEXLIB_HASH_TABLE_WRITER_H
#define __INDEXLIB_HASH_TABLE_WRITER_H

#include <memory>

#include "indexlib/common/hash_table/hash_table_node.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

template <typename Key, typename Value>
class HashTableWriter
{
public:
    HashTableWriter() : mNodeCount(0) {};
    ~HashTableWriter() {};

    void Init(const file_system::DirectoryPtr& rootDir, const std::string& filePath, size_t bucketCount);
    const file_system::FileWriterPtr& GetFileWriter() const { return mFileWriter; };

    void Add(const Key& key, const Value& value);
    void Close();

    static size_t GetDumpSize(size_t bucketCount, size_t itemCount)
    {
        return sizeof(HashTableNode<Key, Value>) * itemCount // itemSize
               + sizeof(uint32_t) * bucketCount              // bucketSize
               + sizeof(uint32_t) * 2;                       // sizeof : nodeCount + bucketCount
    }

    bool Empty() const { return mNodeCount == 0; }

    size_t Size() const { return mNodeCount; }

private:
    uint32_t mNodeCount;
    file_system::FileWriterPtr mFileWriter;

    std::vector<uint32_t> mBucket;
    // private:
    //    IE_LOG_DECLARE();
};

template <typename Key, typename Value>
void HashTableWriter<Key, Value>::Init(const file_system::DirectoryPtr& rootDir, const std::string& filePath,
                                       size_t bucketCount)
{
    mFileWriter = rootDir->CreateFileWriter(filePath);
    mBucket.resize(bucketCount, INVALID_HASHTABLE_OFFSET);
}

template <typename Key, typename Value>
void HashTableWriter<Key, Value>::Add(const Key& key, const Value& value)
{
    assert(mFileWriter);
    size_t bucketIdx = key % mBucket.size();
    HashTableNode<Key, Value> node(key, value, mBucket[bucketIdx]);
    mFileWriter->Write(&node, sizeof(node)).GetOrThrow();
    mBucket[bucketIdx] = mNodeCount++;
}

template <typename Key, typename Value>
void HashTableWriter<Key, Value>::Close()
{
    assert(mFileWriter);
    mFileWriter->Write(mBucket.data(), mBucket.size() * sizeof(uint32_t)).GetOrThrow();
    mFileWriter->Write(&mNodeCount, sizeof(mNodeCount)).GetOrThrow();
    uint32_t bucketCount = mBucket.size();
    mFileWriter->Write(&bucketCount, sizeof(bucketCount)).GetOrThrow();
    mFileWriter->Close().GetOrThrow();
    mFileWriter.reset();
}
}} // namespace indexlib::common

#endif //__INDEXLIB_HASH_TABLE_WRITER_H

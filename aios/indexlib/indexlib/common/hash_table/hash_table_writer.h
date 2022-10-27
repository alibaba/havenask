#ifndef __INDEXLIB_HASH_TABLE_WRITER_H
#define __INDEXLIB_HASH_TABLE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_writer.h"
#include "indexlib/common/hash_table/hash_table_node.h"

IE_NAMESPACE_BEGIN(common);

template<typename Key, typename Value>
class HashTableWriter
{
public:
    HashTableWriter()
        : mNodeCount(0)
    {};
    ~HashTableWriter() { };

    void Init(const file_system::DirectoryPtr& rootDir,
              const std::string& filePath, size_t bucketCount);
    const file_system::FileWriterPtr& GetFileWriter() const
    { return mFileWriter; };

    void Add(const Key& key, const Value& value);
    void Close();

    static size_t GetDumpSize(size_t bucketCount, size_t itemCount)
    { 
        return sizeof(HashTableNode<Key, Value>) * itemCount // itemSize
            + sizeof(uint32_t) * bucketCount // bucketSize
            + sizeof(uint32_t) * 2; // sizeof : nodeCount + bucketCount
    }

    bool Empty() const
    { return mNodeCount == 0; }

    size_t Size() const
    { return mNodeCount; }

private:
    uint32_t mNodeCount;
    file_system::FileWriterPtr mFileWriter;

    std::vector<uint32_t> mBucket;
//private:
//    IE_LOG_DECLARE();
};

template<typename Key, typename Value>
void HashTableWriter<Key, Value>::Init(const file_system::DirectoryPtr& rootDir,
              const std::string& filePath, size_t bucketCount)
{
    mFileWriter = rootDir->CreateFileWriter(filePath);
    mBucket.resize(bucketCount, INVALID_HASHTABLE_OFFSET);
}

template<typename Key, typename Value>
void HashTableWriter<Key, Value>::Add(const Key& key, const Value& value)
{
    assert(mFileWriter);
    size_t bucketIdx = key % mBucket.size();
    HashTableNode<Key, Value> node(key, value, mBucket[bucketIdx]);
    mFileWriter->Write(&node, sizeof(node));
    mBucket[bucketIdx] = mNodeCount++;
}

template<typename Key, typename Value>
void HashTableWriter<Key, Value>::Close()
{
    assert(mFileWriter);
    mFileWriter->Write(mBucket.data(), mBucket.size() * sizeof(uint32_t));
    mFileWriter->Write(&mNodeCount, sizeof(mNodeCount));
    uint32_t bucketCount = mBucket.size();
    mFileWriter->Write(&bucketCount, sizeof(bucketCount));
    mFileWriter->Close();
    mFileWriter.reset();
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_HASH_TABLE_WRITER_H

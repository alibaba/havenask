#ifndef __INDEXLIB_HASH_TABLE_READER_H
#define __INDEXLIB_HASH_TABLE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/file_reader.h"
#include "indexlib/common/hash_table/hash_table_node.h"

IE_NAMESPACE_BEGIN(common);

template<typename Key, typename Value>
class HashTableReader
{
public:
    HashTableReader()
        : mNodeCount(0)
        , mBucketCount(0)
        , mBucketBase(NULL)
        , mNodeBase(NULL)
    {};
    
    ~HashTableReader() { };
    
public:
    void Open(const file_system::DirectoryPtr& rootDir, const std::string& filePath);
    bool Find(const Key& key, Value& value);
    const Value* Find(const Key& key) const;

    uint32_t GetNodeCount() const { return mNodeCount; }
    uint32_t GetBucketCount() const { return mBucketCount; }
    size_t GetFileLength() const { assert(mFileReader); return mFileReader->GetLength(); }
    HashTableNode<Key, Value> GetNode(size_t idx)
    { return mNodeBase[idx]; }

    static void DecodeMeta(const file_system::FileReaderPtr& fileReader,
                           uint32_t &nodeSize, uint32_t &bucketCount);
    
private:
    typedef HashTableNode<Key, Value> NodeType;
    uint32_t mNodeCount;
    uint32_t mBucketCount;
    uint32_t *mBucketBase;
    NodeType *mNodeBase;

    file_system::FileReaderPtr mFileReader;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE2(util, HashTableReader);

template<typename Key, typename Value>
void HashTableReader<Key, Value>::Open(const file_system::DirectoryPtr& rootDir, const std::string& filePath)
{
    mFileReader = rootDir->CreateFileReader(filePath, file_system::FSOT_LOAD_CONFIG);
    DecodeMeta(mFileReader, mNodeCount, mBucketCount);
    try
    {
        mNodeBase = static_cast<NodeType*>(mFileReader->GetBaseAddress());
    }
    catch(const misc::ExceptionBase& e)
    {
        IE_LOG(ERROR, "HashTableReader not support FSOpenType [%d]", mFileReader->GetOpenType());
        throw e;
    }

    if (mNodeBase == NULL)
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "HashTableReader not support FSOpenType [%d]",
                             mFileReader->GetOpenType());
    }
    mBucketBase = reinterpret_cast<uint32_t*>(mNodeBase + mNodeCount);
}

template<typename Key, typename Value>
bool HashTableReader<Key, Value>::Find(const Key& key, Value& value)
{
    uint32_t bucketIdx = key % mBucketCount;
    uint32_t offset = mBucketBase[bucketIdx];
    while (offset != INVALID_HASHTABLE_OFFSET)
    {
        const NodeType& node = mNodeBase[offset];
        if (node.key == key)
        {
            value = node.value;
            return true;
        }
        offset = node.next;
    }
    return false;
}

template<typename Key, typename Value>
const Value* HashTableReader<Key, Value>::Find(const Key& key) const
{
    uint32_t bucketIdx = key % mBucketCount;
    uint32_t offset = mBucketBase[bucketIdx];
    while (offset != INVALID_HASHTABLE_OFFSET)
    {
        const NodeType& node = mNodeBase[offset];
        if (node.key == key)
        {
            return &(node.value);
        }
        offset = node.next;
    }
    return NULL;
}

template<typename Key, typename Value>
inline void HashTableReader<Key, Value>::DecodeMeta(
        const file_system::FileReaderPtr& fileReader,
        uint32_t &nodeSize, uint32_t &bucketCount)
{
    assert(fileReader);
    size_t fileLength = fileReader->GetLength();
    if (fileLength < sizeof(uint32_t) + sizeof(uint32_t))
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "HashTableReader Open Failed,"
                             " file[%s] length[%lu] is smaller than [%lu]",
                             fileReader->GetPath().c_str(),
                             fileLength, sizeof(uint32_t) + sizeof(uint32_t));
    }

    size_t cursor = fileLength - sizeof(uint32_t) - sizeof(uint32_t);
    fileReader->Read(&nodeSize, sizeof(uint32_t), cursor);
    cursor += sizeof(uint32_t);
    fileReader->Read(&bucketCount, sizeof(uint32_t), cursor);

    size_t totalLength = nodeSize * sizeof(NodeType) + bucketCount * sizeof(uint32_t)
        + sizeof(uint32_t) + sizeof(uint32_t);
    if (totalLength != fileLength)
    {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "HashTableReader Open Failed,"
                             " file[%s] length[%lu] error, expect length[%lu]",
                             fileReader->GetPath().c_str(), fileLength, totalLength);
    }
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_HASH_TABLE_READER_H

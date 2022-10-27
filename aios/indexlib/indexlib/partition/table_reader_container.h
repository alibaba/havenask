#ifndef __INDEXLIB_TABLE_READER_CONTAINER_H
#define __INDEXLIB_TABLE_READER_CONTAINER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <unordered_map>
#include <autil/Lock.h>

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);

IE_NAMESPACE_BEGIN(partition);

class TableReaderContainer
{
public:
    TableReaderContainer();
    ~TableReaderContainer();
public:
    void Init(const std::unordered_map<std::string, uint32_t>& tableName2PartitionIdMap)
    {
        autil::ScopedLock lock(mReaderLock);
        mTableName2PartitionIdMap = tableName2PartitionIdMap;
        mReaders.resize(mTableName2PartitionIdMap.size());
    }
    void UpdateReader(const std::string& tableName, const IndexPartitionReaderPtr& reader)
    {
        autil::ScopedLock lock(mReaderLock);
        int32_t idx = GetReaderIdx(tableName);
        assert(idx != -1);
        mReaders[idx] = reader;
    }
    void UpdateReaders(const std::vector<std::string>& tableNames,
                       const std::vector<IndexPartitionReaderPtr> readers)
    {
        autil::ScopedLock lock(mReaderLock);
        assert(tableNames.size() == readers.size());
        for (size_t i = 0; i < tableNames.size(); i++)
        {
            int32_t idx = GetReaderIdx(tableNames[i]);
            assert(idx != -1);
            mReaders[idx] = readers[i];
        }
    }

    IndexPartitionReaderPtr GetReader(const std::string& tableName) const
    {
        autil::ScopedLock lock(mReaderLock);
        int32_t idx = GetReaderIdx(tableName);
        assert(idx != -1);
        return mReaders[idx];
    }

    int32_t GetReaderIdx(const std::string& tableName) const
    {
        auto iter = mTableName2PartitionIdMap.find(tableName);
        if (iter == mTableName2PartitionIdMap.end())
        {
            return -1;
        }
        return iter->second;
    }
    IndexPartitionReaderPtr GetReader(uint32_t idx) const
    {
        autil::ScopedLock lock(mReaderLock);
        if (idx >= mReaders.size())
        {
            return IndexPartitionReaderPtr();
        }
        return mReaders[idx];
    }

    void CreateSnapshot(std::vector<IndexPartitionReaderPtr>& readers) const
    {
        autil::ScopedLock lock(mReaderLock);
        readers = mReaders;
    }
    size_t Size() const { return mReaders.size(); }
    void Clear()
    {
        autil::ScopedLock lock(mReaderLock);
        mReaders.clear();
        mReaders.resize(mTableName2PartitionIdMap.size());
    }

private:
    std::unordered_map<std::string, uint32_t> mTableName2PartitionIdMap;
    std::vector<IndexPartitionReaderPtr> mReaders;
    mutable autil::ThreadMutex mReaderLock;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableReaderContainer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_TABLE_READER_CONTAINER_H

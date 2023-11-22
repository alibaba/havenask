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
#include <unordered_map>

#include "autil/Lock.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);

namespace indexlib { namespace partition {

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
    void UpdateReaders(const std::vector<std::string>& tableNames, const std::vector<IndexPartitionReaderPtr> readers)
    {
        autil::ScopedLock lock(mReaderLock);
        assert(tableNames.size() == readers.size());
        for (size_t i = 0; i < tableNames.size(); i++) {
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
        if (iter == mTableName2PartitionIdMap.end()) {
            return -1;
        }
        return iter->second;
    }
    IndexPartitionReaderPtr GetReader(uint32_t idx) const
    {
        autil::ScopedLock lock(mReaderLock);
        if (idx >= mReaders.size()) {
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
}} // namespace indexlib::partition

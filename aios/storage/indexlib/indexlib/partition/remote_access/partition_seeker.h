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
#ifndef __INDEXLIB_INDEX_PARTITION_SEEKER_H
#define __INDEXLIB_INDEX_PARTITION_SEEKER_H

#include <memory>
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/remote_access/attribute_data_seeker.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, OfflinePartition);

namespace indexlib { namespace partition {

class PartitionSeeker
{
public:
    PartitionSeeker() : mEnableCountedMultiValue(true) {}

    ~PartitionSeeker() { Reset(); }

public:
    bool Init(const partition::OfflinePartitionPtr& partition);

    const IndexPartitionReaderPtr& GetReader() const { return mReader; }

    // should be used in one thread, inner handle life-cycle
    AttributeDataSeeker* GetSingleAttributeSeeker(const std::string& attrName);

    void SetEnableCountedMultiValue(bool enable) { mEnableCountedMultiValue = enable; }
    bool EnableCountedMultiValue() const { return mEnableCountedMultiValue; }

    void Reset();

    size_t GetUsedBytes() const
    {
        if (!mPool) {
            return 0;
        }
        return mPool->getUsedBytes();
    }

public:
    // life-cycle control by pool
    AttributeDataSeeker* CreateSingleAttributeSeeker(const std::string& attrName, autil::mem_pool::Pool* pool);

private:
    typedef std::unordered_map<std::string, AttributeDataSeeker*> SeekerMap;
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;

private:
    SeekerMap mSeekerMap;
    IndexPartitionReaderPtr mReader;
    PoolPtr mPool;
    autil::ThreadMutex mLock;
    bool mEnableCountedMultiValue;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionSeeker);
}} // namespace indexlib::partition

#endif //__INDEXLIB_INDEX_PARTITION_SEEKER_H

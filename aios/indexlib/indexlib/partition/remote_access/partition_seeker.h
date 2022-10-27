#ifndef __INDEXLIB_INDEX_PARTITION_SEEKER_H
#define __INDEXLIB_INDEX_PARTITION_SEEKER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/remote_access/attribute_data_seeker.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(partition, OfflinePartition);

IE_NAMESPACE_BEGIN(partition);

class PartitionSeeker
{
public:
    PartitionSeeker()
        : mEnableCountedMultiValue(true)
    {}
    
    ~PartitionSeeker()
    {
        Reset();
    }
    
public:
    bool Init(const partition::OfflinePartitionPtr& partition);

    const IndexPartitionReaderPtr &GetReader() const {
        return mReader;
    }

    // should be used in one thread, inner handle life-cycle
    AttributeDataSeeker* GetSingleAttributeSeeker(const std::string& attrName);

    void SetEnableCountedMultiValue(bool enable) { mEnableCountedMultiValue = enable; }
    bool EnableCountedMultiValue() const { return mEnableCountedMultiValue; }
    
    void Reset();
    
    size_t GetUsedBytes() const
    {
        if (!mPool)
        {
            return 0;
        }
        return mPool->getUsedBytes();
    }

public:
    // life-cycle control by pool
    AttributeDataSeeker* CreateSingleAttributeSeeker(
            const std::string& attrName, autil::mem_pool::Pool* pool);
    
private:
    typedef std::tr1::unordered_map<std::string, AttributeDataSeeker*> SeekerMap;
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    
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

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_PARTITION_SEEKER_H

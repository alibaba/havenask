#ifndef __INDEXLIB_SHARDING_PARTITIONER_H
#define __INDEXLIB_SHARDING_PARTITIONER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_hasher_typed.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);

IE_NAMESPACE_BEGIN(index);

class ShardingPartitioner
{
public:
    ShardingPartitioner();
    virtual ~ShardingPartitioner();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              uint32_t shardingColumnCount);
    
    bool GetShardingIdx(const autil::ConstString& key, uint32_t& shardingIdx)
    {
        assert(mKeyHasherType != hft_unknown);
        dictkey_t hashValue;
        if (!util::GetHashKey(mKeyHasherType, key, hashValue))
        {
            IE_LOG(WARN, "get hash key[%s] failed", key.c_str());
            ERROR_COLLECTOR_LOG(WARN, "get hash key[%s] failed", key.c_str());
            return false;
        }
        shardingIdx = hashValue & mShardingMask;
        return true;
    }
    
    virtual bool GetShardingIdx(const document::DocumentPtr& doc, uint32_t& shardingIdx) = 0;
    
private:
    virtual void InitKeyHasherType(const config::IndexPartitionSchemaPtr& schema) = 0;
    
protected:
    HashFunctionType mKeyHasherType;
    uint64_t mShardingMask;
    bool mIsMultiRegion;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShardingPartitioner);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SHARDING_PARTITIONER_H

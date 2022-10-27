#ifndef __INDEXLIB_KV_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_KV_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/config/kv_index_preference.h"

IE_NAMESPACE_BEGIN(config);

class KVIndexConfigImpl : public SingleFieldIndexConfigImpl
{
public:
    KVIndexConfigImpl(const std::string& indexName, IndexType indexType);    
    ~KVIndexConfigImpl();

public:
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override;
    IndexConfigImpl* Clone() const override;
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    const ValueConfigPtr& GetValueConfig() const { return mValueConfig; }
    void SetValueConfig(const ValueConfigPtr& valueConfig)
    { mValueConfig = valueConfig; }

    bool TTLEnabled() const { return mTTL >= 0; }
    void DisableTTL() { mTTL = INVALID_TTL; }
    int64_t GetTTL() const { return TTLEnabled() ? mTTL : DEFAULT_TIME_TO_LIVE; }
    void SetTTL(int64_t ttl)
    { mTTL = ttl >= 0 ? ttl : DEFAULT_TIME_TO_LIVE; }
    
    size_t GetRegionCount() const { return mRegionCount; }
    regionid_t GetRegionId() const { return mRegionId; }

    const std::string& GetKeyFieldName() const;
    const KVIndexPreference& GetIndexPreference() const { return mIndexPreference; }
    KVIndexPreference& GetIndexPreference() { return mIndexPreference; }

    void SetIndexPreference(const KVIndexPreference& indexPreference)
    { mIndexPreference = indexPreference; }

    void SetUseSwapMmapFile(bool useSwapMmapFile)
    { mUseSwapMmapFile = useSwapMmapFile; }
    bool GetUseSwapMmapFile() { return mUseSwapMmapFile; }

    void SetMaxSwapMmapFileSize(int64_t maxSwapMmapFileSize)
    { mMaxSwapMmapFileSize = maxSwapMmapFileSize; }
    int64_t GetMaxSwapMmapFileSize() const { return mMaxSwapMmapFileSize; }

    void SetRegionInfo(regionid_t regionId, size_t regionCount)
    {
        mRegionId = regionId;
        mRegionCount = regionCount;
    }
    
    bool UseNumberHash() const { return mUseNumberHash; }

public:
    static HashFunctionType GetHashFunctionType(FieldType fieldType, bool useNumberHash);
    
private:
    bool CheckFieldType(FieldType ft) const override { return true; }

private:
    ValueConfigPtr mValueConfig;
    KVIndexPreference mIndexPreference;
    int64_t mTTL;

protected:
    bool mUseNumberHash;
    bool mUseSwapMmapFile;
    int64_t mMaxSwapMmapFileSize;
    regionid_t mRegionId;
    size_t mRegionCount;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KV_INDEX_CONFIG_IMPL_H

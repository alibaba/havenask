#ifndef __INDEXLIB_KV_INDEX_CONFIG_H
#define __INDEXLIB_KV_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/config/kv_index_preference.h"

DECLARE_REFERENCE_CLASS(config, KVIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

class KVIndexConfig : public SingleFieldIndexConfig
{
public:
    KVIndexConfig(const std::string& indexName, IndexType indexType);
    KVIndexConfig(const KVIndexConfig& other);    
    ~KVIndexConfig();

public:
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    const ValueConfigPtr& GetValueConfig() const;
    void SetValueConfig(const ValueConfigPtr& valueConfig);

    bool TTLEnabled() const;
    void DisableTTL();
    int64_t GetTTL() const;
    void SetTTL(int64_t ttl);
    
    size_t GetRegionCount() const;
    regionid_t GetRegionId() const;

    const std::string& GetKeyFieldName() const;
    const KVIndexPreference& GetIndexPreference() const;
    KVIndexPreference& GetIndexPreference();

    void SetIndexPreference(const KVIndexPreference& indexPreference);

    HashFunctionType GetKeyHashFunctionType() const;
    void SetUseSwapMmapFile(bool useSwapMmapFile);
    bool GetUseSwapMmapFile();

    void SetMaxSwapMmapFileSize(int64_t maxSwapMmapFileSize);
    int64_t GetMaxSwapMmapFileSize() const;

    void SetRegionInfo(regionid_t regionId, size_t regionCount);    
    bool UseNumberHash() const;
    bool IsCompactHashKey() const;

protected:
    void ResetImpl(IndexConfigImpl* impl);

public:
    static HashFunctionType GetHashFunctionType(FieldType fieldType, bool useNumberHash);
private:
    KVIndexConfigImpl* mImpl;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KV_INDEX_CONFIG_H

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
#ifndef __INDEXLIB_KV_INDEX_CONFIG_H
#define __INDEXLIB_KV_INDEX_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlib { namespace config {

class KVIndexConfig : public SingleFieldIndexConfig
{
public:
    KVIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    KVIndexConfig(const KVIndexConfig& other);
    ~KVIndexConfig();

public:
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    IndexConfig* Clone() const override;
    void Check() const override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& GetIndexType() const override { return indexlibv2::index::KV_INDEX_TYPE_STR; };

    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override
    {
        assert(false);
        return nullptr;
    }

public:
    const ValueConfigPtr& GetValueConfig() const;
    void SetValueConfig(const ValueConfigPtr& valueConfig);

    bool TTLEnabled() const;
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
    bool GetUseSwapMmapFile() const;

    void SetMaxSwapMmapFileSize(int64_t maxSwapMmapFileSize);
    int64_t GetMaxSwapMmapFileSize() const;

    void SetRegionInfo(regionid_t regionId, size_t regionCount);
    bool UseNumberHash() const;
    void SetUseNumberHash(bool useNumberHash);
    bool IsCompactHashKey() const;

    void EnableStoreExpireTime();
    bool StoreExpireTime() const;

    std::unique_ptr<indexlibv2::config::KVIndexConfig> MakeKVIndexConfigV2() const;
    void FillKVIndexConfigV2(indexlibv2::config::KVIndexConfig* configV2) const;

public:
    static HashFunctionType GetHashFunctionType(FieldType fieldType, bool useNumberHash);

private:
    bool CheckFieldType(FieldType ft) const override;
    void InitStorageKVOptimize();

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVIndexConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_KV_INDEX_CONFIG_H

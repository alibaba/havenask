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

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/kv/Types.h"
#include "indexlib/index/kv/config/TTLSettings.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlib::config {
class KVIndexPreference;
}

namespace indexlibv2::config {
class FieldConfig;

class KVIndexConfig : public IIndexConfig
{
public:
    KVIndexConfig();
    ~KVIndexConfig();
    KVIndexConfig(const KVIndexConfig&);

public:
    const std::string& GetIndexType() const override;
    const std::string& GetIndexName() const override;
    const std::string& GetIndexCommonPath() const override;
    std::vector<std::string> GetIndexPath() const override;
    std::vector<std::shared_ptr<FieldConfig>> GetFieldConfigs() const override;
    void Check() const override;
    void Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                     const config::IndexConfigDeserializeResource& resource) override;
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckCompatible(const IIndexConfig* other) const override;
    bool IsDisabled() const override;

public:
    void SetIndexName(const std::string& indexName);
    std::shared_ptr<FieldConfig> GetFieldConfig() const;
    void SetFieldConfig(const std::shared_ptr<FieldConfig>& keyFieldConfig);

public:
    const std::shared_ptr<ValueConfig>& GetValueConfig() const;
    void SetValueConfig(const std::shared_ptr<ValueConfig>& valueConfig);

    bool TTLEnabled() const;
    int64_t GetTTL() const;
    void SetTTL(int64_t ttl);

    const std::string& GetKeyFieldName() const;
    const indexlib::config::KVIndexPreference& GetIndexPreference() const;
    indexlib::config::KVIndexPreference& GetIndexPreference();

    void SetIndexPreference(const indexlib::config::KVIndexPreference& indexPreference);

    indexlib::HashFunctionType GetKeyHashFunctionType() const;
    void SetUseSwapMmapFile(bool useSwapMmapFile);
    bool GetUseSwapMmapFile() const;

    void SetMaxSwapMmapFileSize(int64_t maxSwapMmapFileSize);
    int64_t GetMaxSwapMmapFileSize() const;

    bool UseNumberHash() const;
    void SetUseNumberHash(bool useNumberHash);
    bool IsCompactHashKey() const;

    void EnableStoreExpireTime();
    bool StoreExpireTime() const;

    void SetTTLSettings(const std::shared_ptr<TTLSettings>& settings);
    const std::shared_ptr<TTLSettings>& GetTTLSettings() const;

    bool DenyEmptyPrimaryKey() const;
    void SetDenyEmptyPrimaryKey(bool flag);

    bool IgnoreEmptyPrimaryKey() const;
    void SetIgnoreEmptyPrimaryKey(bool flag);

public:
    static indexlib::HashFunctionType GetHashFunctionType(FieldType fieldType, bool useNumberHash);

protected:
    inline static const std::string INDEX_NAME = "index_name";
    inline static const std::string INDEX_PREFERENCE_NAME = "index_preference";
    inline static const std::string DEFAULT_TTL = "default_ttl";
    inline static const std::string STORE_EXPIRE_TIME = "store_expire_time";
    inline static const std::string USE_NUMBER_HASH = "use_number_hash";
    inline static const std::string USE_SWAP_MMAP_FILE = "use_swap_mmap_file";
    inline static const std::string MAX_SWAP_MMAP_FILE_SIZE = "max_swap_mmap_file_size";
    inline static const std::string VALUE_FIELDS = "value_fields";
    inline static const std::string KEY_FIELD = "key_field";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config

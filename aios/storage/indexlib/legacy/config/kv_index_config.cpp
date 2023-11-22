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
#include "indexlib/config/kv_index_config.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, KVIndexConfig);

struct KVIndexConfig::Impl {
    ValueConfigPtr valueConfig;
    KVIndexPreference indexPreference;
    int64_t defaultTTL = INVALID_TTL;
    bool storeExpireTime = false;
    bool useNumberHash = true;
    bool useSwapMmapFile = false;
    int64_t maxSwapMmapFileSize = -1;
    regionid_t regionId = INVALID_REGIONID;
    size_t regionCount = 0;
};

KVIndexConfig::KVIndexConfig(const string& indexName, InvertedIndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(std::make_unique<Impl>())
{
    // attention: set property format version id according to function log
    [[maybe_unused]] auto status = SetMaxSupportedIndexFormatVersionId(0);
    indexlibv2::config::InvertedIndexConfig::SetOptionFlag(of_none);
}

KVIndexConfig::KVIndexConfig(const KVIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(std::make_unique<Impl>(*other.mImpl))
{
}

KVIndexConfig::~KVIndexConfig() {};

void KVIndexConfig::AssertEqual(const IndexConfig& other) const
{
    IndexConfig::AssertEqual(other);
    const auto& kvIndexConfig = dynamic_cast<const KVIndexConfig&>(other);

    IE_CONFIG_ASSERT_EQUAL(mImpl->defaultTTL, kvIndexConfig.mImpl->defaultTTL, "ttl not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->storeExpireTime, kvIndexConfig.mImpl->storeExpireTime, "store_expire_time not equal");
    // TODO: how about value other config
}
void KVIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    SingleFieldIndexConfig::AssertCompatible(other);
}

IndexConfig* KVIndexConfig::Clone() const { return new KVIndexConfig(*this); }

void KVIndexConfig::Check() const
{
    auto singleFieldConfig = GetFieldConfig();
    if (singleFieldConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "key not support multi_value field!");
    }

    IndexConfig::Check();
    mImpl->indexPreference.Check();
    if (mImpl->indexPreference.GetHashDictParam().GetHashType() != "dense" &&
        mImpl->indexPreference.GetHashDictParam().GetHashType() != "cuckoo") {
        INDEXLIB_FATAL_ERROR(Schema, "key only support dense or cuckoo now");
    }
}

void KVIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfig::Jsonize(json);
    json.Jsonize(INDEX_PREFERENCE_NAME, mImpl->indexPreference, mImpl->indexPreference);
    if (json.GetMode() == TO_JSON) {
        json.Jsonize(USE_NUMBER_HASH, mImpl->useNumberHash);
    } else {
        InitStorageKVOptimize();
        json.Jsonize(USE_NUMBER_HASH, mImpl->useNumberHash, mImpl->useNumberHash);
    }
}

void KVIndexConfig::InitStorageKVOptimize()
{
    if (!autil::EnvUtil::getEnv("INDEXLIB_OPT_KV_STORE", false)) {
        return;
    }

    auto valueParam = mImpl->indexPreference.GetValueParam();
    if (!valueParam.GetFileCompressType().empty()) {
        valueParam.SetCompressParameter("encode_address_mapper", "true");
    }
    valueParam.EnableValueImpact(true);
    mImpl->indexPreference.SetValueParam(valueParam);
}

const ValueConfigPtr& KVIndexConfig::GetValueConfig() const { return mImpl->valueConfig; }

void KVIndexConfig::SetValueConfig(const ValueConfigPtr& valueConfig) { mImpl->valueConfig = valueConfig; }

bool KVIndexConfig::TTLEnabled() const { return mImpl->defaultTTL != INVALID_TTL; }

int64_t KVIndexConfig::GetTTL() const { return mImpl->defaultTTL; }

void KVIndexConfig::SetTTL(int64_t ttl) { mImpl->defaultTTL = ttl; }

size_t KVIndexConfig::GetRegionCount() const { return mImpl->regionCount; }
regionid_t KVIndexConfig::GetRegionId() const { return mImpl->regionId; }

const std::string& KVIndexConfig::GetKeyFieldName() const
{
    auto fieldConfig = GetFieldConfig();
    return fieldConfig->GetFieldName();
}

const KVIndexPreference& KVIndexConfig::GetIndexPreference() const { return mImpl->indexPreference; }

KVIndexPreference& KVIndexConfig::GetIndexPreference() { return mImpl->indexPreference; }

void KVIndexConfig::SetIndexPreference(const KVIndexPreference& indexPreference)
{
    mImpl->indexPreference = indexPreference;
    auto valueConfig = GetValueConfig();
    if (valueConfig) {
        valueConfig->EnableValueImpact(indexPreference.GetValueParam().IsValueImpact());
        valueConfig->EnablePlainFormat(indexPreference.GetValueParam().IsPlainFormat());
    }
}

HashFunctionType KVIndexConfig::GetKeyHashFunctionType() const
{
    return GetHashFunctionType(GetFieldConfig()->GetFieldType(), mImpl->useNumberHash);
}
void KVIndexConfig::SetUseSwapMmapFile(bool useSwapMmapFile) { mImpl->useSwapMmapFile = useSwapMmapFile; }
bool KVIndexConfig::GetUseSwapMmapFile() const { return mImpl->useSwapMmapFile; }

void KVIndexConfig::SetMaxSwapMmapFileSize(int64_t maxSwapMmapFileSize)
{
    mImpl->maxSwapMmapFileSize = maxSwapMmapFileSize;
}
int64_t KVIndexConfig::GetMaxSwapMmapFileSize() const { return mImpl->maxSwapMmapFileSize; }

void KVIndexConfig::SetRegionInfo(regionid_t regionId, size_t regionCount)
{
    mImpl->regionId = regionId;
    mImpl->regionCount = regionCount;
}
bool KVIndexConfig::UseNumberHash() const { return mImpl->useNumberHash; }
void KVIndexConfig::SetUseNumberHash(bool useNumberHash) { mImpl->useNumberHash = useNumberHash; }
bool KVIndexConfig::IsCompactHashKey() const
{
    KVIndexPreference preference = GetIndexPreference();
    if (!preference.GetHashDictParam().HasEnableCompactHashKey()) {
        return false;
    }
    HashFunctionType hashType = GetKeyHashFunctionType();
    if (hashType == hft_murmur) {
        return false;
    }
    assert(hashType == hft_int64 || hashType == hft_uint64);
    FieldType ft = GetFieldConfig()->GetFieldType();
    switch (ft) {
    case ft_int8:
    case ft_int16:
    case ft_int32:
    case ft_uint8:
    case ft_uint16:
    case ft_uint32:
        return true;
    default:
        break;
    }
    return false;
}

void KVIndexConfig::EnableStoreExpireTime()
{
    if (TTLEnabled()) {
        mImpl->storeExpireTime = true;
    }
}

bool KVIndexConfig::StoreExpireTime() const { return mImpl->storeExpireTime; }

HashFunctionType KVIndexConfig::GetHashFunctionType(FieldType fieldType, bool useNumberHash)
{
    if (!useNumberHash) {
        return hft_murmur;
    }
    switch (fieldType) {
    case ft_int8:
    case ft_int16:
    case ft_int32:
    case ft_int64:
        return hft_int64;
    case ft_uint8:
    case ft_uint16:
    case ft_uint32:
    case ft_uint64:
        return hft_uint64;
    default:
        return hft_murmur;
    }
}

bool KVIndexConfig::CheckFieldType(FieldType ft) const { return true; }

std::unique_ptr<indexlibv2::config::KVIndexConfig> KVIndexConfig::MakeKVIndexConfigV2() const
{
    auto kvConfigV2 = std::make_unique<indexlibv2::config::KVIndexConfig>();
    FillKVIndexConfigV2(kvConfigV2.get());
    return kvConfigV2;
}

void KVIndexConfig::FillKVIndexConfigV2(indexlibv2::config::KVIndexConfig* configV2) const
{
    assert(configV2);
    configV2->SetIndexName(GetIndexName());
    configV2->SetFieldConfig(GetFieldConfig());
    configV2->SetValueConfig(GetValueConfig());
    configV2->SetIndexPreference(GetIndexPreference());
    configV2->SetTTL(GetTTL());
    if (StoreExpireTime()) {
        configV2->EnableStoreExpireTime();
    }
    configV2->SetUseNumberHash(UseNumberHash());
    configV2->SetUseSwapMmapFile(GetUseSwapMmapFile());
    configV2->SetMaxSwapMmapFileSize(GetMaxSwapMmapFileSize());
}

}} // namespace indexlib::config

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
#include "indexlib/index/kv/config/KVIndexConfig.h"

#include "autil/HashAlgorithm.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/util/Exception.h"

using indexlib::config::KVIndexPreference;
using std::shared_ptr;
using std::string;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, KVIndexConfig);

struct KVIndexConfig::Impl {
    string indexName;
    std::shared_ptr<FieldConfig> keyFieldConfig;
    std::shared_ptr<ValueConfig> valueConfig;
    KVIndexPreference indexPreference;
    int64_t defaultTTL = indexlib::INVALID_TTL;
    bool storeExpireTime = false;
    bool useNumberHash = true;
    bool useSwapMmapFile = false;
    int64_t maxSwapMmapFileSize = -1;
    std::shared_ptr<TTLSettings> ttlSettings;
    bool denyEmptyPrimaryKey = false;
    bool ignoreEmptyPrimaryKey = false;

    Impl() { ttlSettings = std::make_shared<TTLSettings>(); }
};

KVIndexConfig::KVIndexConfig() : _impl(std::make_unique<Impl>()) {}
KVIndexConfig::KVIndexConfig(const KVIndexConfig& other) : _impl(std::make_unique<Impl>(*(other._impl))) {}

KVIndexConfig::~KVIndexConfig() {}

const string& KVIndexConfig::GetIndexType() const { return indexlibv2::index::KV_INDEX_TYPE_STR; }
const string& KVIndexConfig::GetIndexName() const { return _impl->indexName; }

void KVIndexConfig::SetIndexName(const std::string& indexName) { _impl->indexName = indexName; }

const string& KVIndexConfig::GetIndexCommonPath() const { return indexlibv2::index::KV_INDEX_PATH; }
std::vector<string> KVIndexConfig::GetIndexPath() const { return {GetIndexCommonPath() + "/" + GetIndexName()}; }
void KVIndexConfig::Check() const
{
    assert(_impl->keyFieldConfig);
    if (_impl->keyFieldConfig->IsMultiValue()) {
        INDEXLIB_FATAL_ERROR(Schema, "key not support multi_value field!");
    }

    _impl->indexPreference.Check();
    if (_impl->indexPreference.GetHashDictParam().GetHashType() != "dense" &&
        _impl->indexPreference.GetHashDictParam().GetHashType() != "cuckoo") {
        INDEXLIB_FATAL_ERROR(Schema, "key only support dense or cuckoo now");
    }
}

std::vector<std::shared_ptr<FieldConfig>> KVIndexConfig::GetFieldConfigs() const { return {GetFieldConfig()}; }

std::shared_ptr<FieldConfig> KVIndexConfig::GetFieldConfig() const { return _impl->keyFieldConfig; }

void KVIndexConfig::SetFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    _impl->keyFieldConfig = fieldConfig;
}

void KVIndexConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                const config::IndexConfigDeserializeResource& resource)
{
    autil::legacy::Jsonizable::JsonWrapper json(any);
    json.Jsonize(INDEX_NAME, _impl->indexName, _impl->indexName);
    json.Jsonize(INDEX_PREFERENCE_NAME, _impl->indexPreference, _impl->indexPreference);
    json.Jsonize(DEFAULT_TTL, _impl->defaultTTL, _impl->defaultTTL);
    json.Jsonize(STORE_EXPIRE_TIME, _impl->storeExpireTime, _impl->storeExpireTime);
    json.Jsonize(USE_NUMBER_HASH, _impl->useNumberHash, _impl->useNumberHash);
    json.Jsonize(USE_SWAP_MMAP_FILE, _impl->useSwapMmapFile, _impl->useSwapMmapFile);
    json.Jsonize(MAX_SWAP_MMAP_FILE_SIZE, _impl->maxSwapMmapFileSize, _impl->maxSwapMmapFileSize);

    std::string keyFieldName;
    json.Jsonize(KEY_FIELD, keyFieldName, keyFieldName);
    auto keyField = resource.GetFieldConfig(keyFieldName);
    // TODO: add back this check after kkv does not inherit from me
    // if (!keyField) {
    //     INDEXLIB_FATAL_ERROR(Schema, "key field %s does not exist", keyFieldName.c_str());
    // }
    _impl->keyFieldConfig = std::move(keyField);

    std::vector<std::string> valueFields;
    auto valueConfig = std::make_shared<indexlibv2::config::ValueConfig>();
    json.Jsonize(VALUE_FIELDS, valueFields);
    attrid_t attrId = 0;
    std::vector<shared_ptr<indexlibv2::index::AttributeConfig>> attrConfigVec;
    for (const auto& fieldName : valueFields) {
        const auto& fieldConfig = resource.GetFieldConfig(fieldName);
        if (!fieldConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "value field [%s] does not exist", fieldName.c_str());
        }
        auto attrConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
        auto status = attrConfig->Init(fieldConfig);
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(Schema, "KVIndexConfig attrConfig init failed, indexName: %s",
                                 _impl->indexName.c_str());
        }
        attrConfig->SetAttrId(attrId++);
        attrConfigVec.emplace_back(attrConfig);
    }
    auto status = valueConfig->Init(attrConfigVec);
    if (!status.IsOK()) {
        INDEXLIB_FATAL_ERROR(Schema, "KVIndexConfig valueConfig init failed, indexName: %s", _impl->indexName.c_str());
    }
    valueConfig->EnableValueImpact(GetIndexPreference().GetValueParam().IsValueImpact());
    valueConfig->EnablePlainFormat(GetIndexPreference().GetValueParam().IsPlainFormat());
    valueConfig->EnableCompactFormat(true);
    _impl->valueConfig = valueConfig;
}

void KVIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    json.Jsonize(INDEX_NAME, _impl->indexName, _impl->indexName);
    json.Jsonize(INDEX_PREFERENCE_NAME, _impl->indexPreference, _impl->indexPreference);
    json.Jsonize(DEFAULT_TTL, _impl->defaultTTL, _impl->defaultTTL);
    json.Jsonize(STORE_EXPIRE_TIME, _impl->storeExpireTime, _impl->storeExpireTime);
    json.Jsonize(USE_NUMBER_HASH, _impl->useNumberHash, _impl->useNumberHash);
    json.Jsonize(USE_SWAP_MMAP_FILE, _impl->useSwapMmapFile, _impl->useSwapMmapFile);
    json.Jsonize(MAX_SWAP_MMAP_FILE_SIZE, _impl->maxSwapMmapFileSize, _impl->maxSwapMmapFileSize);
    std::string fieldName = _impl->keyFieldConfig->GetFieldName();
    json.Jsonize(KEY_FIELD, fieldName);
    if (_impl->valueConfig) {
        const auto& valueConfig = _impl->valueConfig;
        std::vector<std::string> fields;
        auto fieldCount = valueConfig->GetAttributeCount();
        fields.reserve(fieldCount);
        for (size_t i = 0; i < fieldCount; ++i) {
            fields.push_back(valueConfig->GetAttributeConfig(i)->GetFieldConfig()->GetFieldName());
        }
        if (!fields.empty()) {
            json.Jsonize(VALUE_FIELDS, fields);
        }
    }
}

const shared_ptr<ValueConfig>& KVIndexConfig::GetValueConfig() const { return _impl->valueConfig; }

void KVIndexConfig::SetValueConfig(const shared_ptr<ValueConfig>& valueConfig) { _impl->valueConfig = valueConfig; }

bool KVIndexConfig::TTLEnabled() const { return _impl->defaultTTL != indexlib::INVALID_TTL; }

int64_t KVIndexConfig::GetTTL() const { return _impl->defaultTTL; }

void KVIndexConfig::SetTTL(int64_t ttl) { _impl->defaultTTL = ttl; }

const std::string& KVIndexConfig::GetKeyFieldName() const
{
    auto keyFieldConfig = GetFieldConfig();
    return keyFieldConfig->GetFieldName();
}

const KVIndexPreference& KVIndexConfig::GetIndexPreference() const { return _impl->indexPreference; }

KVIndexPreference& KVIndexConfig::GetIndexPreference() { return _impl->indexPreference; }

void KVIndexConfig::SetIndexPreference(const KVIndexPreference& indexPreference)
{
    _impl->indexPreference = indexPreference;
    auto valueConfig = GetValueConfig();
    if (valueConfig) {
        valueConfig->EnableValueImpact(indexPreference.GetValueParam().IsValueImpact());
        valueConfig->EnablePlainFormat(indexPreference.GetValueParam().IsPlainFormat());
    }
}

indexlib::HashFunctionType KVIndexConfig::GetKeyHashFunctionType() const
{
    return GetHashFunctionType(GetFieldConfig()->GetFieldType(), _impl->useNumberHash);
}
void KVIndexConfig::SetUseSwapMmapFile(bool useSwapMmapFile) { _impl->useSwapMmapFile = useSwapMmapFile; }
bool KVIndexConfig::GetUseSwapMmapFile() const { return _impl->useSwapMmapFile; }

void KVIndexConfig::SetMaxSwapMmapFileSize(int64_t maxSwapMmapFileSize)
{
    _impl->maxSwapMmapFileSize = maxSwapMmapFileSize;
}
int64_t KVIndexConfig::GetMaxSwapMmapFileSize() const { return _impl->maxSwapMmapFileSize; }

bool KVIndexConfig::UseNumberHash() const { return _impl->useNumberHash; }
void KVIndexConfig::SetUseNumberHash(bool useNumberHash) { _impl->useNumberHash = useNumberHash; }
bool KVIndexConfig::IsCompactHashKey() const
{
    KVIndexPreference preference = GetIndexPreference();
    if (!preference.GetHashDictParam().HasEnableCompactHashKey()) {
        return false;
    }
    indexlib::HashFunctionType hashType = GetKeyHashFunctionType();
    if (hashType == indexlib::hft_murmur) {
        return false;
    }
    assert(hashType == indexlib::hft_int64 || hashType == indexlib::hft_uint64);
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
        _impl->storeExpireTime = true;
    }
}

bool KVIndexConfig::StoreExpireTime() const { return _impl->storeExpireTime; }

void KVIndexConfig::SetTTLSettings(const std::shared_ptr<TTLSettings>& settings) { _impl->ttlSettings = settings; }
const std::shared_ptr<TTLSettings>& KVIndexConfig::GetTTLSettings() const { return _impl->ttlSettings; }

bool KVIndexConfig::DenyEmptyPrimaryKey() const { return _impl->denyEmptyPrimaryKey; }
void KVIndexConfig::SetDenyEmptyPrimaryKey(bool flag) { _impl->denyEmptyPrimaryKey = flag; }

bool KVIndexConfig::IgnoreEmptyPrimaryKey() const { return _impl->ignoreEmptyPrimaryKey; }
void KVIndexConfig::SetIgnoreEmptyPrimaryKey(bool flag) { _impl->ignoreEmptyPrimaryKey = flag; }

indexlib::HashFunctionType KVIndexConfig::GetHashFunctionType(FieldType fieldType, bool useNumberHash)
{
    if (!useNumberHash) {
        return indexlib::hft_murmur;
    }
    switch (fieldType) {
    case ft_int8:
    case ft_int16:
    case ft_int32:
    case ft_int64:
        return indexlib::hft_int64;
    case ft_uint8:
    case ft_uint16:
    case ft_uint32:
    case ft_uint64:
        return indexlib::hft_uint64;
    default:
        return indexlib::hft_murmur;
    }
}

Status KVIndexConfig::CheckCompatible(const IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const KVIndexConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to KVIndexConfig failed");
    }
    CHECK_CONFIG_EQUAL(_impl->indexName, typedOther->_impl->indexName, "index name not equal");
    CHECK_CONFIG_EQUAL(_impl->keyFieldConfig->GetFieldName(), typedOther->_impl->keyFieldConfig->GetFieldName(),
                       "key field is not same");
    // std::set<std::string> valueFields;
    // const auto& valueConfig = _impl->valueConfig;
    // for (size_t i = 0; i < valueConfig->GetAttributeCount(); ++i) {
    //     valueFields.insert(valueConfig->GetAttributeConfig(i)->GetFieldConfig()->GetFieldName());
    // }
    // const auto& valueConfigOther = typedOther->_impl->valueConfig;
    // for (size_t i = 0; i < valueConfigOther->GetAttributeCount(); ++i) {
    //     const auto& fieldConfig = valueConfigOther->GetAttributeConfig(i)->GetFieldConfig();
    //     const auto& fieldName = fieldConfig->GetFieldName();
    //     if (valueFields.find(fieldName) != valueFields.end()) {
    //         continue;
    //     }
    //     if (fieldConfig->GetDefaultValue() == FieldConfig::FIELD_DEFAULT_STR_VALUE) {
    //         RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "new added value field [%s] not set default value",
    //                                fieldName.c_str());
    //     }
    // }
    CHECK_CONFIG_EQUAL(_impl->defaultTTL, typedOther->_impl->defaultTTL, "default ttl not equal");
    CHECK_CONFIG_EQUAL(_impl->storeExpireTime, typedOther->_impl->storeExpireTime, "store expire time  not equal");
    CHECK_CONFIG_EQUAL(_impl->useNumberHash, typedOther->_impl->useNumberHash, "use number hash not equal");
    CHECK_CONFIG_EQUAL(_impl->useSwapMmapFile, typedOther->_impl->useSwapMmapFile, "use swap mmap file not equal");
    CHECK_CONFIG_EQUAL(_impl->maxSwapMmapFileSize, typedOther->_impl->maxSwapMmapFileSize,
                       "max swap mmap file size not equal");
    return Status::OK();
}

bool KVIndexConfig::IsDisabled() const { return false; }

} // namespace indexlibv2::config

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
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/primary_key/Constant.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using indexlib::config::PrimaryKeyLoadStrategyParam;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyIndexConfig);

struct PrimaryKeyIndexConfig::Impl {
    mutable std::optional<PrimaryKeyLoadStrategyParam> pkLoadParam;
    mutable std::shared_ptr<index::AttributeConfig> pkAttributeConfig;
    PrimaryKeyIndexType pkIndexType = pk_sort_array;
    PrimaryKeyHashType pkHashType = pk_default_hash;
    int32_t pkDataBlockSize = DEFAULT_PK_DATA_BLOCK_SIZE;
    bool hasPKAttribute = false;
    /*inByte*/ /*used for block array type, should be 2^n, such as 4096*/
    uint32_t bloomFilterMultipleNum = 0;
    bool paralllelLookupOnBuild = false;
};

PrimaryKeyIndexConfig::PrimaryKeyIndexConfig(const std::string& indexName, InvertedIndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , _impl(make_unique<Impl>())
{
}

PrimaryKeyIndexConfig::PrimaryKeyIndexConfig(const PrimaryKeyIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , _impl(make_unique<Impl>(*(other._impl)))
{
}

PrimaryKeyIndexConfig::~PrimaryKeyIndexConfig() {}

PrimaryKeyIndexConfig& PrimaryKeyIndexConfig::operator=(const PrimaryKeyIndexConfig& other)
{
    if (this != &other) {
        SingleFieldIndexConfig::operator=(other);
        _impl = std::make_unique<Impl>(*(other._impl));
    }
    return *this;
}

void PrimaryKeyIndexConfig::SetPrimaryKeyAttributeFlag(bool flag) { _impl->hasPKAttribute = flag; }

PrimaryKeyIndexConfig* PrimaryKeyIndexConfig::Clone() const { return new PrimaryKeyIndexConfig(*this); }

void PrimaryKeyIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    SingleFieldIndexConfig::Serialize(json);
    json.Jsonize("pk_data_block_size", _impl->pkDataBlockSize);

    string pkIndexTypeStr = PkIndexTypeToString(_impl->pkIndexType);
    json.Jsonize(config::PRIMARY_KEY_STORAGE_TYPE, pkIndexTypeStr);

    string pkHashTypeStr = PkHashTypeToString(_impl->pkHashType);
    json.Jsonize("pk_hash_type", pkHashTypeStr);
    json.Jsonize("has_primary_key_attribute", _impl->hasPKAttribute);
}

Status PrimaryKeyIndexConfig::CheckEqual(const InvertedIndexConfig& other) const
{
    auto status = SingleFieldIndexConfig::CheckEqual(other);
    RETURN_IF_STATUS_ERROR(status, "check field config failed");
    const PrimaryKeyIndexConfig& other2 = (const PrimaryKeyIndexConfig&)other;
    CHECK_CONFIG_EQUAL(_impl->pkIndexType, other2._impl->pkIndexType, "_impl->pkIndexType not equal");
    CHECK_CONFIG_EQUAL(_impl->pkHashType, other2._impl->pkHashType, "_impl->pkHashType not equal");
    CHECK_CONFIG_EQUAL(_impl->pkDataBlockSize, other2._impl->pkDataBlockSize, "mPkDataBlockSIze not equal");
    return GetPKLoadStrategyParam().CheckEqual(other2.GetPKLoadStrategyParam());
}

// not call SingleFieldIndexConfig as for some check not suitable for pk
void PrimaryKeyIndexConfig::Check() const
{
    if (!IsPrimaryKeyIndex()) {
        INDEXLIB_FATAL_ERROR(Schema, "wrong pk index type");
    }

    if (IsIndexUpdatable()) {
        INDEXLIB_FATAL_ERROR(Schema, "pk not support update");
    }

    if (GetIndexFormatVersionId() > GetMaxSupportedIndexFormatVersionId()) {
        INDEXLIB_FATAL_ERROR(Schema, "pk format_verison_id [%d] over max supported value [%d]",
                             GetIndexFormatVersionId(), GetMaxSupportedIndexFormatVersionId());
    }

    auto fileCompress = GetFileCompressConfig();
    if (fileCompress && !fileCompress->GetCompressType().empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "pk not support file compress");
    }

    CheckWhetherIsVirtualField();

    if (_impl->pkHashType == pk_number_hash) {
        if (GetFieldConfig()->IsMultiValue() || !config::FieldConfig::IsIntegerType(GetFieldConfig()->GetFieldType())) {
            INDEXLIB_FATAL_ERROR(Schema, "enable number pk hash, but pk field type not number"
                                         " or multivalue");
        }
    }
    if (_impl->pkIndexType != pk_sort_array &&
        GetPKLoadStrategyParam().GetPrimaryKeyLoadMode() == PrimaryKeyLoadStrategyParam::SORTED_VECTOR) {
        INDEXLIB_FATAL_ERROR(Schema, "SORTED_VECTOR load moad only support pk index type of sort_array");
    }
    if (_impl->pkIndexType != pk_block_array &&
        GetPKLoadStrategyParam().GetPrimaryKeyLoadMode() == PrimaryKeyLoadStrategyParam::BLOCK_VECTOR) {
        INDEXLIB_FATAL_ERROR(Schema, "BLOCK_VECTOR load moad only support pk index type of block_array");
    }
    if (GetFieldConfig()->IsEnableNullField()) {
        INDEXLIB_FATAL_ERROR(Schema, "primary key index not support enable null");
    }
}

Status PrimaryKeyIndexConfig::SetPrimaryKeyLoadParam(PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
                                                     const std::string& param)
{
    _impl->pkLoadParam = PrimaryKeyLoadStrategyParam(loadMode);
    return _impl->pkLoadParam.value().FromString(param);
}

void PrimaryKeyIndexConfig::SetPrimaryKeyLoadParam(const indexlib::config::PrimaryKeyLoadStrategyParam& param)
{
    _impl->pkLoadParam = param;
}

const PrimaryKeyLoadStrategyParam& PrimaryKeyIndexConfig::GetPKLoadStrategyParam() const
{
    if (!_impl->pkLoadParam) {
        _impl->pkLoadParam = PrimaryKeyLoadStrategyParam();
    }
    return _impl->pkLoadParam.value();
}
void PrimaryKeyIndexConfig::SetPrimaryKeyIndexType(PrimaryKeyIndexType type)
{
    _impl->pkIndexType = type;
    SetDefaultPrimaryKeyLoadParamFromPkIndexType();
}

PrimaryKeyIndexType PrimaryKeyIndexConfig::GetPrimaryKeyIndexType() const { return _impl->pkIndexType; }
void PrimaryKeyIndexConfig::SetPrimaryKeyHashType(PrimaryKeyHashType type) { _impl->pkHashType = type; }
PrimaryKeyHashType PrimaryKeyIndexConfig::GetPrimaryKeyHashType() const { return _impl->pkHashType; }
void PrimaryKeyIndexConfig::SetPrimaryKeyDataBlockSize(int32_t pkDataBlockSize)
{
    _impl->pkDataBlockSize = pkDataBlockSize;
}

bool PrimaryKeyIndexConfig::GetBloomFilterParamForPkReader(uint32_t& multipleNum, uint32_t& hashFuncNum) const
{
    if (_impl->pkIndexType == pk_hash_table || _impl->bloomFilterMultipleNum == 0 ||
        _impl->bloomFilterMultipleNum == 1) {
        return false;
    }

    assert(_impl->bloomFilterMultipleNum > 1);
    multipleNum = _impl->bloomFilterMultipleNum;
    hashFuncNum = InvertedIndexConfig::GetHashFuncNumForBloomFilter(_impl->bloomFilterMultipleNum);
    return true;
}

void PrimaryKeyIndexConfig::EnableBloomFilterForPkReader(uint32_t multipleNum)
{
    assert(multipleNum <= 16);
    _impl->bloomFilterMultipleNum = multipleNum; // 0 or 1 means disable bloom filter
}

bool PrimaryKeyIndexConfig::IsParallelLookupOnBuild() const { return _impl->paralllelLookupOnBuild; }
void PrimaryKeyIndexConfig::EnableParallelLookupOnBuild() { _impl->paralllelLookupOnBuild = true; }

int32_t PrimaryKeyIndexConfig::GetPrimaryKeyDataBlockSize() const { return _impl->pkDataBlockSize; }

bool PrimaryKeyIndexConfig::IsPrimaryKeyIndex() const
{
    return GetInvertedIndexType() == it_primarykey64 || GetInvertedIndexType() == it_primarykey128;
}

void PrimaryKeyIndexConfig::DisablePrimaryKeyCombineSegments()
{
    if (!_impl->pkLoadParam) {
        return;
    }
    _impl->pkLoadParam.value().DisableCombineSegments();
    AUTIL_LOG(INFO, "disable pk combine segment");
}

void PrimaryKeyIndexConfig::SetDefaultPrimaryKeyLoadParamFromPkIndexType()
{
    if (_impl->pkLoadParam) {
        return;
    }
    AUTIL_LOG(DEBUG, "prepare default pkLoadParam from pkIndexType");
    if (_impl->pkIndexType == pk_hash_table) {
        _impl->pkLoadParam = PrimaryKeyLoadStrategyParam(PrimaryKeyLoadStrategyParam::HASH_TABLE);
    } else if (_impl->pkIndexType == pk_block_array) {
        _impl->pkLoadParam = PrimaryKeyLoadStrategyParam(PrimaryKeyLoadStrategyParam::BLOCK_VECTOR);
    } else if (_impl->pkIndexType == pk_sort_array) {
        _impl->pkLoadParam = PrimaryKeyLoadStrategyParam(PrimaryKeyLoadStrategyParam::SORTED_VECTOR);
    }
}
PrimaryKeyIndexType PrimaryKeyIndexConfig::StringToPkIndexType(const string& strPkIndexType)
{
    PrimaryKeyIndexType type = pk_sort_array;
    if (strPkIndexType == "hash_table") {
        type = pk_hash_table;
    } else if (strPkIndexType == "block_array") {
        type = pk_block_array;
    } else if (strPkIndexType == "sort_array") {
        type = pk_sort_array;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "unsupported pk storage type: %s", strPkIndexType.c_str());
    }
    return type;
}

string PrimaryKeyIndexConfig::PkIndexTypeToString(const PrimaryKeyIndexType type)
{
    switch (type) {
    case pk_hash_table:
        return "hash_table";
    case pk_sort_array:
        return "sort_array";
    case pk_block_array:
        return "block_array";
    default:
        INDEXLIB_FATAL_ERROR(UnSupported, "unknown pk type[%d]!", type);
    }
    return string("");
}

PrimaryKeyHashType PrimaryKeyIndexConfig::StringToPkHashType(const string& strPkHashType)
{
    PrimaryKeyHashType type = pk_default_hash;
    if (strPkHashType == "default_hash") {
        type = pk_default_hash;
    } else if (strPkHashType == "murmur_hash") {
        type = pk_murmur_hash;
    } else if (strPkHashType == "number_hash") {
        type = pk_number_hash;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "unsupported pk hash type: %s", strPkHashType.c_str());
    }
    return type;
}

string PrimaryKeyIndexConfig::PkHashTypeToString(const PrimaryKeyHashType type)
{
    switch (type) {
    case pk_default_hash:
        return string("default_hash");
    case pk_murmur_hash:
        return string("murmur_hash");
    case pk_number_hash:
        return string("number_hash");
    default:
        INDEXLIB_FATAL_ERROR(UnSupported, "unknown pk hash type[%d]!", type);
    }
    return string("");
}

bool PrimaryKeyIndexConfig::CheckFieldType(FieldType ft) const { return true; }

void PrimaryKeyIndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                          const config::IndexConfigDeserializeResource& resource)
{
    SingleFieldIndexConfig::DoDeserialize(any, resource);
    autil::legacy::Jsonizable::JsonWrapper jsonWrapper(any);
    jsonWrapper.Jsonize("pk_data_block_size", _impl->pkDataBlockSize, _impl->pkDataBlockSize);
    string pkIndexTypeStr = "sort_array";
    jsonWrapper.Jsonize(config::PRIMARY_KEY_STORAGE_TYPE, pkIndexTypeStr, pkIndexTypeStr);
    _impl->pkIndexType = StringToPkIndexType(pkIndexTypeStr);
    SetDefaultPrimaryKeyLoadParamFromPkIndexType();
    string pkHashTypeStr = "default_hash";
    jsonWrapper.Jsonize("pk_hash_type", pkHashTypeStr, pkHashTypeStr);
    _impl->pkHashType = StringToPkHashType(pkHashTypeStr);
    bool useNumberPkHash = false;
    jsonWrapper.Jsonize(config::USE_NUMBER_PK_HASH, useNumberPkHash, false);
    jsonWrapper.Jsonize("has_primary_key_attribute", _impl->hasPKAttribute, _impl->hasPKAttribute);
    if (useNumberPkHash) {
        _impl->pkHashType = pk_number_hash;
    }
    SetIndexId(INVALID_INDEXID);
}
bool PrimaryKeyIndexConfig::HasPrimaryKeyAttribute() const { return _impl->hasPKAttribute; }

std::optional<PrimaryKeyLoadStrategyParam> PrimaryKeyIndexConfig::TEST_GetPrimaryKeyLoadStrategyParam() const
{
    return _impl->pkLoadParam;
}

std::shared_ptr<index::AttributeConfig> PrimaryKeyIndexConfig::GetPKAttributeConfig() const
{
    if (HasPrimaryKeyAttribute() && !_impl->pkAttributeConfig) {
        auto attributeConfig = std::make_shared<indexlibv2::index::AttributeConfig>();
        FieldType fieldType = GetInvertedIndexType() == it_primarykey64 ? ft_uint64 : ft_hash_128;
        auto fieldConfig = std::make_shared<indexlibv2::config::FieldConfig>(
            std::string(indexlib::index::PRIMARY_KEY_ATTRIBUTE_PREFIX) + "_" + GetFieldConfig()->GetFieldName(),
            fieldType, false);
        [[maybe_unused]] auto status = attributeConfig->Init(fieldConfig);
        assert(status.IsOK());
        attributeConfig->SetUpdatable(false);
        _impl->pkAttributeConfig = attributeConfig;
    }
    return _impl->pkAttributeConfig;
}

} // namespace indexlibv2::index

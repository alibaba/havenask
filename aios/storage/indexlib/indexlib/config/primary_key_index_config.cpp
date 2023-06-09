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
#include "indexlib/config/primary_key_index_config.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, PrimaryKeyIndexConfig);

struct PrimaryKeyIndexConfig::Impl {
    mutable PrimaryKeyLoadStrategyParamPtr pkLoadParam;
    PrimaryKeyIndexType pkIndexType = pk_sort_array;
    index::PrimaryKeyHashType pkHashType = index::pk_default_hash;
    int32_t pkDataBlockSize = DEFAULT_PK_DATA_BLOCK_SIZE;
    /*inByte*/ /*used for block array type, should be 2^n, such as 4096*/
    uint32_t bloomFilterMultipleNum = 0;
    bool paralllelLookupOnBuild = false;
};

PrimaryKeyIndexConfig::PrimaryKeyIndexConfig(const std::string& indexName, InvertedIndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(make_unique<Impl>())
{
}

PrimaryKeyIndexConfig::PrimaryKeyIndexConfig(const PrimaryKeyIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
{
}

PrimaryKeyIndexConfig::~PrimaryKeyIndexConfig() {}

IndexConfig* PrimaryKeyIndexConfig::Clone() const { return new PrimaryKeyIndexConfig(*this); }

void PrimaryKeyIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfig::Jsonize(json);
    json.Jsonize("pk_data_block_size", mImpl->pkDataBlockSize, mImpl->pkDataBlockSize);
    if (json.GetMode() == TO_JSON) {
        string pkIndexTypeStr = PkIndexTypeToString(mImpl->pkIndexType);
        json.Jsonize(PRIMARY_KEY_STORAGE_TYPE, pkIndexTypeStr);

        string pkHashTypeStr = PkHashTypeToString(mImpl->pkHashType);
        json.Jsonize("pk_hash_type", pkHashTypeStr);
    } else {
        string pkIndexTypeStr = "sort_array";
        json.Jsonize(PRIMARY_KEY_STORAGE_TYPE, pkIndexTypeStr, pkIndexTypeStr);
        mImpl->pkIndexType = StringToPkIndexType(pkIndexTypeStr);
        SetDefaultPrimaryKeyLoadParamFromPkIndexType();
        string pkHashTypeStr = "default_hash";
        json.Jsonize("pk_hash_type", pkHashTypeStr, pkHashTypeStr);
        mImpl->pkHashType = StringToPkHashType(pkHashTypeStr);

        bool useNumberPkHash = false;
        json.Jsonize(USE_NUMBER_PK_HASH, useNumberPkHash, false);
        if (useNumberPkHash) {
            mImpl->pkHashType = index::pk_number_hash;
        }
    }
}
void PrimaryKeyIndexConfig::AssertEqual(const IndexConfig& other) const
{
    SingleFieldIndexConfig::AssertEqual(other);
    const PrimaryKeyIndexConfig& other2 = (const PrimaryKeyIndexConfig&)other;
    IE_CONFIG_ASSERT_EQUAL(mImpl->pkIndexType, other2.mImpl->pkIndexType, "mImpl->pkIndexType not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->pkHashType, other2.mImpl->pkHashType, "mImpl->pkHashType not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->pkDataBlockSize, other2.mImpl->pkDataBlockSize, "mPkDataBlockSIze not equal");
    auto status = GetPKLoadStrategyParam()->CheckEqual(*other2.mImpl->pkLoadParam);
    THROW_IF_STATUS_ERROR(status);
}
void PrimaryKeyIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    SingleFieldIndexConfig::AssertEqual(other);
    const PrimaryKeyIndexConfig& other2 = (const PrimaryKeyIndexConfig&)other;
    IE_CONFIG_ASSERT_EQUAL(mImpl->pkIndexType, other2.mImpl->pkIndexType, "mImpl->pkIndexType not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->pkHashType, other2.mImpl->pkHashType, "mImpl->pkHashType not equal");
}
void PrimaryKeyIndexConfig::Check() const
{
    SingleFieldIndexConfig::Check();
    if (!IsPrimaryKeyIndex()) {
        INDEXLIB_FATAL_ERROR(Schema, "wrong pk index type");
    }
    if (mImpl->pkHashType == index::pk_number_hash) {
        if (GetFieldConfig()->IsMultiValue() || !FieldConfig::IsIntegerType(GetFieldConfig()->GetFieldType())) {
            INDEXLIB_FATAL_ERROR(Schema, "enable number pk hash, but pk field type not number"
                                         " or multivalue");
        }
    }
    if (mImpl->pkIndexType != pk_sort_array &&
        GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode() == PrimaryKeyLoadStrategyParam::SORTED_VECTOR) {
        INDEXLIB_FATAL_ERROR(Schema, "SORTED_VECTOR load moad only support pk index type of sort_array");
    }
    if (mImpl->pkIndexType != pk_block_array &&
        GetPKLoadStrategyParam()->GetPrimaryKeyLoadMode() == PrimaryKeyLoadStrategyParam::BLOCK_VECTOR) {
        INDEXLIB_FATAL_ERROR(Schema, "BLOCK_VECTOR load moad only support pk index type of block_array");
    }
    if (GetFieldConfig()->IsEnableNullField()) {
        INDEXLIB_FATAL_ERROR(Schema, "primary key index not support enable null");
    }
}
void PrimaryKeyIndexConfig::SetPrimaryKeyLoadParam(PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
                                                   bool lookupReverse, const std::string& param)
{
    mImpl->pkLoadParam.reset(new PrimaryKeyLoadStrategyParam(loadMode, lookupReverse));
    auto status = mImpl->pkLoadParam->FromString(param);
    THROW_IF_STATUS_ERROR(status);
}
PrimaryKeyLoadStrategyParamPtr PrimaryKeyIndexConfig::GetPKLoadStrategyParam() const
{
    if (!mImpl->pkLoadParam) {
        mImpl->pkLoadParam.reset(new PrimaryKeyLoadStrategyParam());
    }
    return mImpl->pkLoadParam;
}
void PrimaryKeyIndexConfig::SetPrimaryKeyIndexType(PrimaryKeyIndexType type)
{
    mImpl->pkIndexType = type;
    SetDefaultPrimaryKeyLoadParamFromPkIndexType();
}
PrimaryKeyIndexType PrimaryKeyIndexConfig::GetPrimaryKeyIndexType() const { return mImpl->pkIndexType; }
void PrimaryKeyIndexConfig::SetPrimaryKeyHashType(index::PrimaryKeyHashType type) { mImpl->pkHashType = type; }
index::PrimaryKeyHashType PrimaryKeyIndexConfig::GetPrimaryKeyHashType() const { return mImpl->pkHashType; }
void PrimaryKeyIndexConfig::SetPrimaryKeyDataBlockSize(int32_t pkDataBlockSize)
{
    mImpl->pkDataBlockSize = pkDataBlockSize;
}

bool PrimaryKeyIndexConfig::GetBloomFilterParamForPkReader(uint32_t& multipleNum, uint32_t& hashFuncNum) const
{
    if (mImpl->pkIndexType == pk_hash_table || mImpl->bloomFilterMultipleNum == 0 ||
        mImpl->bloomFilterMultipleNum == 1) {
        return false;
    }

    assert(mImpl->bloomFilterMultipleNum > 1);
    multipleNum = mImpl->bloomFilterMultipleNum;
    hashFuncNum = IndexConfig::GetHashFuncNumForBloomFilter(mImpl->bloomFilterMultipleNum);
    return true;
}

void PrimaryKeyIndexConfig::EnableBloomFilterForPkReader(uint32_t multipleNum)
{
    assert(multipleNum <= 16);
    mImpl->bloomFilterMultipleNum = multipleNum; // 0 or 1 means disable bloom filter
}

bool PrimaryKeyIndexConfig::IsParallelLookupOnBuild() const { return mImpl->paralllelLookupOnBuild; }
void PrimaryKeyIndexConfig::EnableParallelLookupOnBuild() { mImpl->paralllelLookupOnBuild = true; }

int32_t PrimaryKeyIndexConfig::GetPrimaryKeyDataBlockSize() const { return mImpl->pkDataBlockSize; }

bool PrimaryKeyIndexConfig::IsPrimaryKeyIndex() const
{
    return GetInvertedIndexType() == it_primarykey64 || GetInvertedIndexType() == it_primarykey128;
}

void PrimaryKeyIndexConfig::SetDefaultPrimaryKeyLoadParamFromPkIndexType()
{
    if (mImpl->pkLoadParam) {
        return;
    }
    IE_LOG(INFO, "prepare default pkLoadParam from pkIndexType");
    if (mImpl->pkIndexType == pk_hash_table) {
        mImpl->pkLoadParam.reset(new PrimaryKeyLoadStrategyParam(PrimaryKeyLoadStrategyParam::HASH_TABLE));
    } else if (mImpl->pkIndexType == pk_block_array) {
        mImpl->pkLoadParam.reset(new PrimaryKeyLoadStrategyParam(PrimaryKeyLoadStrategyParam::BLOCK_VECTOR));
    } else if (mImpl->pkIndexType == pk_sort_array) {
        mImpl->pkLoadParam.reset(new PrimaryKeyLoadStrategyParam(PrimaryKeyLoadStrategyParam::SORTED_VECTOR));
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

index::PrimaryKeyHashType PrimaryKeyIndexConfig::StringToPkHashType(const string& strPkHashType)
{
    index::PrimaryKeyHashType type = index::pk_default_hash;
    if (strPkHashType == "default_hash") {
        type = index::pk_default_hash;
    } else if (strPkHashType == "murmur_hash") {
        type = index::pk_murmur_hash;
    } else if (strPkHashType == "number_hash") {
        type = index::pk_number_hash;
    } else {
        INDEXLIB_FATAL_ERROR(Schema, "unsupported pk hash type: %s", strPkHashType.c_str());
    }
    return type;
}

string PrimaryKeyIndexConfig::PkHashTypeToString(const index::PrimaryKeyHashType type)
{
    switch (type) {
    case index::pk_default_hash:
        return string("default_hash");
    case index::pk_murmur_hash:
        return string("murmur_hash");
    case index::pk_number_hash:
        return string("number_hash");
    default:
        INDEXLIB_FATAL_ERROR(UnSupported, "unknown pk hash type[%d]!", type);
    }
    return string("");
}

bool PrimaryKeyIndexConfig::CheckFieldType(FieldType ft) const { return true; }

std::unique_ptr<indexlibv2::index::PrimaryKeyIndexConfig> PrimaryKeyIndexConfig::MakePrimaryIndexConfigV2() const
{
    auto configV2 = ConstructConfigV2();
    if (!configV2) {
        IE_LOG(ERROR, "construct primary key index config v2 failed");
        return nullptr;
    }
    return std::unique_ptr<indexlibv2::index::PrimaryKeyIndexConfig>(
        dynamic_cast<indexlibv2::index::PrimaryKeyIndexConfig*>(configV2.release()));
}

std::unique_ptr<indexlibv2::config::InvertedIndexConfig> PrimaryKeyIndexConfig::ConstructConfigV2() const
{
    return DoConstructConfigV2<indexlibv2::index::PrimaryKeyIndexConfig>();
}

bool PrimaryKeyIndexConfig::FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const
{
    if (!SingleFieldIndexConfig::FulfillConfigV2(configV2)) {
        IE_LOG(ERROR, "fulfill primary index config failed");
        return false;
    }
    auto typedConfigV2 = dynamic_cast<indexlibv2::index::PrimaryKeyIndexConfig*>(configV2);
    assert(typedConfigV2);
    typedConfigV2->SetPrimaryKeyIndexType(mImpl->pkIndexType);
    const auto& loadStrategyParam = GetPKLoadStrategyParam();
    if (loadStrategyParam) {
        typedConfigV2->SetPrimaryKeyLoadParam(*loadStrategyParam);
    }
    typedConfigV2->SetPrimaryKeyHashType(mImpl->pkHashType);
    typedConfigV2->SetPrimaryKeyDataBlockSize(mImpl->pkDataBlockSize);
    typedConfigV2->EnableBloomFilterForPkReader(mImpl->bloomFilterMultipleNum);
    if (IsParallelLookupOnBuild()) {
        typedConfigV2->EnableParallelLookupOnBuild();
    }

    bool hasPKAttribute = HasPrimaryKeyAttribute();
    typedConfigV2->SetPrimaryKeyAttributeFlag(hasPKAttribute);
    return true;
}

}} // namespace indexlib::config

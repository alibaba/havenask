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
#include "indexlib/config/attribute_config.h"

#include <memory>

#include "indexlib/base/Constant.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib { namespace config {
IE_LOG_SETUP(config, AttributeConfig);

using namespace indexlib::common;

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

struct AttributeConfig::Impl {
    // legacy framework need legacy field config

    // TODO: not exist in v2, check if migration is necessary
    common::AttributeValueInitializerCreatorPtr valueInitializerCreator;

    // customized config and opid, only exist in legacy framework
    CustomizedConfigVector customizedConfigs;
    schema_opid_t ownerOpId = INVALID_SCHEMA_OP_ID;
};

AttributeConfig::AttributeConfig(ConfigType type)
    : indexlibv2::config::AttributeConfig(type)
    , mImpl(std::make_unique<Impl>())
{
}

AttributeConfig::~AttributeConfig() {}

void AttributeConfig::Init(const FieldConfigPtr& fieldConfig, const AttributeValueInitializerCreatorPtr& creator,
                           const CustomizedConfigVector& customizedConfigs)
{
    SetFieldConfig(fieldConfig);
    mImpl->valueInitializerCreator = creator;
    mImpl->customizedConfigs = customizedConfigs;

    if (fieldConfig->IsMultiValue() || fieldConfig->GetFieldType() == ft_string) {
        indexlibv2::config::AttributeConfig::SetUpdatable(fieldConfig->IsUpdatableMultiValue());
    }
    auto status = indexlibv2::config::AttributeConfig::SetCompressType(fieldConfig->GetCompressType().GetCompressStr());
    THROW_IF_STATUS_ERROR(status);
    indexlibv2::config::AttributeConfig::SetDefragSlicePercent(fieldConfig->GetDefragSlicePercent() * 100);
    indexlibv2::config::AttributeConfig::SetU32OffsetThreshold(fieldConfig->GetU32OffsetThreshold());
}

void AttributeConfig::SetFieldConfig(const FieldConfigPtr& fieldConfig)
{
    assert(fieldConfig.get() != NULL);
    FieldType fieldType = fieldConfig->GetFieldType();
    bool isMultiValue = fieldConfig->IsMultiValue();

    if (fieldType == ft_integer || fieldType == ft_float || fieldType == ft_long || fieldType == ft_fp8 ||
        fieldType == ft_fp16 || fieldType == ft_uint32 || fieldType == ft_uint64 || fieldType == ft_int8 ||
        fieldType == ft_uint8 || fieldType == ft_int16 || fieldType == ft_uint16 || fieldType == ft_double ||
        (fieldType == ft_hash_64 && !isMultiValue) || (fieldType == ft_hash_128 && !isMultiValue) ||
        fieldType == ft_string || fieldType == ft_location || fieldType == ft_line || fieldType == ft_polygon ||
        fieldType == ft_raw || fieldType == ft_time || fieldType == ft_timestamp || fieldType == ft_date) {
        indexlibv2::config::AttributeConfig::Init(fieldConfig);
        return;
    }

    INDEXLIB_FATAL_ERROR(Schema, "Unsupport Attribute: fieldName = %s, fieldType = %s, isMultiValue = %s",
                         fieldConfig->GetFieldName().c_str(), FieldConfig::FieldTypeToStr(fieldType),
                         (isMultiValue ? "true" : "false"));
}

size_t AttributeConfig::EstimateAttributeExpandSize(size_t docCount)
{
    FieldType fieldType = GetFieldConfig()->GetFieldType();
    if (GetFieldConfig()->IsMultiValue() || fieldType == ft_string || fieldType == ft_raw) {
        INDEXLIB_FATAL_ERROR(UnSupported, "can not estimate the size of multivalue attribute or raw attribute");
    }
    return SizeOfFieldType(fieldType) * docCount;
}

FieldConfigPtr AttributeConfig::GetFieldConfig() const
{
    return std::dynamic_pointer_cast<FieldConfig>(indexlibv2::config::AttributeConfig::GetFieldConfig());
}

bool AttributeConfig::IsBuiltInAttribute() const { return GetFieldConfig()->IsBuiltInField(); }

void AttributeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        indexlibv2::config::AttributeConfig::Serialize(json);
        if (!mImpl->customizedConfigs.empty()) {
            json.Jsonize(CUSTOMIZED_CONFIG, mImpl->customizedConfigs);
        }
    }
}

common::AttributeValueInitializerCreatorPtr AttributeConfig::GetAttrValueInitializerCreator() const
{
    return mImpl->valueInitializerCreator;
}

void AttributeConfig::AssertEqual(const AttributeConfig& other) const
{
    auto status = indexlibv2::config::AttributeConfig::AssertEqual(other);
    THROW_IF_STATUS_ERROR(status);

    IE_CONFIG_ASSERT_EQUAL(mImpl->ownerOpId, other.mImpl->ownerOpId, "ownerOpId not equal");

    auto fieldConfig = GetFieldConfig();
    auto otherFieldConfig = other.GetFieldConfig();
    if (fieldConfig && otherFieldConfig) {
        fieldConfig->AssertEqual(*(otherFieldConfig));
    } else if (fieldConfig || otherFieldConfig) {
        INDEXLIB_FATAL_ERROR(AssertEqual, "FieldConfig is not equal");
    }

    auto packAttrConfig = GetPackAttributeConfig();
    auto otherPackAttrConfig = other.GetPackAttributeConfig();
    if (packAttrConfig && otherPackAttrConfig) {
        packAttrConfig->AssertEqual(*otherPackAttrConfig);
    } else if (packAttrConfig || otherPackAttrConfig) {
        INDEXLIB_FATAL_ERROR(AssertEqual, "PackAttrConfig is not equal");
    }

    for (size_t i = 0; i < mImpl->customizedConfigs.size(); i++) {
        mImpl->customizedConfigs[i]->AssertEqual(*(other.mImpl->customizedConfigs[i]));
    }
}

void AttributeConfig::SetCustomizedConfig(const CustomizedConfigVector& customizedConfigs)
{
    mImpl->customizedConfigs = customizedConfigs;
}

const CustomizedConfigVector& AttributeConfig::GetCustomizedConfigs() const { return mImpl->customizedConfigs; }

void AttributeConfig::SetOwnerModifyOperationId(schema_opid_t opId) { mImpl->ownerOpId = opId; }

schema_opid_t AttributeConfig::GetOwnerModifyOperationId() const { return mImpl->ownerOpId; }

void AttributeConfig::SetUpdatableMultiValue(bool isUpdatable)
{
    indexlibv2::config::AttributeConfig::SetUpdatable(isUpdatable);
    GetFieldConfig()->SetUpdatableMultiValue(isUpdatable);
}
Status AttributeConfig::SetCompressType(const std::string& compressStr)
{
    auto status = indexlibv2::config::AttributeConfig::SetCompressType(compressStr);
    RETURN_IF_STATUS_ERROR(status, "attribute config set compress type fail, compressStr[%s]", compressStr.c_str());
    GetFieldConfig()->SetCompressType(compressStr);
    return Status::OK();
}

PackAttributeConfig* AttributeConfig::GetPackAttributeConfig() const
{
    return dynamic_cast<PackAttributeConfig*>(indexlibv2::config::AttributeConfig::GetPackAttributeConfig());
}

bool AttributeConfig::IsLegacyAttributeConfig() const
{
    if (getenv("INDEXLIB_FORCE_USE_LEGACY_ATTRIBUTE_CONFIG")) {
        IE_LOG(WARN, "INDEXLIB_FORCE_USE_LEGACY_ATTRIBUTE_CONFIG is set, will serialize legacy attribute config, some "
                     "property maybe lost, such as updatable");
        return !GetFileCompressConfig() || !GetCustomizedConfigs().empty();
    } else {
        return false;
    }
}

}} // namespace indexlib::config

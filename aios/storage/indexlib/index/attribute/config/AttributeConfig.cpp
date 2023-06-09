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
#include "indexlib/index/attribute/config/AttributeConfig.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, AttributeConfig);

struct AttributeConfig::Impl {
    std::shared_ptr<FieldConfig> fieldConfig;
    attrid_t attrId = 0;
    AttributeConfig::ConfigType configType = ct_normal;
    indexlib::config::CompressTypeOption compressType;
    uint64_t defragSlicePercent = index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT;
    std::shared_ptr<indexlib::config::FileCompressConfig> fileCompressConfig;
    std::shared_ptr<FileCompressConfigV2> fileCompressConfigV2;
    PackAttributeConfig* packAttrConfig = nullptr;
    uint64_t u32OffsetThreshold = index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX;
    indexlib::IndexStatus status = indexlib::is_normal;
    bool updatable = true; // need initialize by FieldConfig
    int64_t sliceCount = 1;
    // not jsonize, no sliceIdx means no slice
    int64_t sliceIdx = -1;
};

AttributeConfig::AttributeConfig(ConfigType type) : _impl(std::make_unique<Impl>()) { _impl->configType = type; }

AttributeConfig::~AttributeConfig() {}

static bool IsUpdatableSingleValue(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    if (fieldConfig->IsMultiValue()) {
        return false;
    }
    auto fieldType = fieldConfig->GetFieldType();
    return fieldType == ft_int32 || fieldType == ft_uint32 || fieldType == ft_int64 || fieldType == ft_uint64 ||
           fieldType == ft_int8 || fieldType == ft_uint8 || fieldType == ft_int16 || fieldType == ft_uint16 ||
           fieldType == ft_double || fieldType == ft_float || fieldType == ft_fp8 || fieldType == ft_fp16 ||
           fieldType == ft_time || fieldType == ft_date || fieldType == ft_timestamp;
}

void AttributeConfig::Init(const std::shared_ptr<FieldConfig>& fieldConfig)
{
    assert(fieldConfig);
    _impl->fieldConfig = fieldConfig;
    _impl->updatable = IsUpdatableSingleValue(fieldConfig); // set DEFAULT, may changed by SetUpdatable()
}

int64_t AttributeConfig::GetSliceCount() const { return _impl->sliceCount; }
int64_t AttributeConfig::GetSliceIdx() const { return _impl->sliceIdx; }

void AttributeConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                  const config::IndexConfigDeserializeResource& resource)
{
    SetAttrId(idxInJsonArray);
    // do not support ["attr1", "attr2"] in new schema no longer
    auto json = autil::legacy::Jsonizable::JsonWrapper(any);

    std::string fieldName;
    json.Jsonize(FieldConfig::FIELD_NAME, fieldName, fieldName);

    if (!_impl->fieldConfig) {
        Init(resource.GetFieldConfig(fieldName)); // indexlib v2
    } else {
        // already Init by legacy indexlib::config::AttributeConfig::SetFieldConfig
    }

    std::string compressType;
    json.Jsonize(index::COMPRESS_TYPE, compressType, compressType);
    if (auto status = _impl->compressType.Init(compressType); !status.IsOK()) {
        INDEXLIB_FATAL_ERROR(Schema, "invalid compressType [%s], status[%s]", compressType.c_str(),
                             status.ToString().c_str());
    }

    // FILE_COMPRESSOR
    std::string fileCompressor;
    json.Jsonize(index::FILE_COMPRESSOR, fileCompressor, fileCompressor);
    if (!fileCompressor.empty()) {
        auto fileCompressConfig = resource.GetFileCompressConfig(fileCompressor);
        if (!fileCompressConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "get file compress[%s] failed, fieldName[%s]", fileCompressor.c_str(),
                                 fieldName.c_str());
        }
        _impl->fileCompressConfigV2 = fileCompressConfig;
    }

    json.Jsonize(index::ATTRIBUTE_U32OFFSET_THRESHOLD, _impl->u32OffsetThreshold, _impl->u32OffsetThreshold);
    json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, _impl->defragSlicePercent, _impl->defragSlicePercent);
    json.Jsonize(index::ATTRIBUTE_UPDATABLE, _impl->updatable, _impl->updatable);
    json.Jsonize(index::ATTRIBUTE_SLICE_COUNT, _impl->sliceCount, _impl->sliceCount);
}

void AttributeConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    json.Jsonize(FieldConfig::FIELD_NAME, GetAttrName());
    if (_impl->fileCompressConfig) {
        std::string compressName = _impl->fileCompressConfig->GetCompressName();
        json.Jsonize(FILE_COMPRESS, compressName);
    } else if (_impl->fileCompressConfigV2) {
        std::string fileCompressor = _impl->fileCompressConfigV2->GetCompressName();
        json.Jsonize(index::FILE_COMPRESSOR, fileCompressor);
    }
    if (_impl->compressType.HasCompressOption()) {
        std::string compressStr = _impl->compressType.GetCompressStr();
        assert(!compressStr.empty());
        json.Jsonize(index::COMPRESS_TYPE, compressStr);
    }
    if (_impl->defragSlicePercent != index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT) {
        json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, _impl->defragSlicePercent);
    }
    if (_impl->u32OffsetThreshold != index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX) {
        json.Jsonize(index::ATTRIBUTE_U32OFFSET_THRESHOLD, _impl->u32OffsetThreshold);
    }
    json.Jsonize(index::ATTRIBUTE_UPDATABLE, _impl->updatable);
    if (_impl->sliceCount > 1) {
        json.Jsonize(index::ATTRIBUTE_SLICE_COUNT, _impl->sliceCount);
    }
}

const std::string& AttributeConfig::GetIndexType() const
{
    switch (_impl->configType) {
    case ConfigType::ct_normal:
    case ConfigType::ct_virtual:
    case ConfigType::ct_section:
    case ConfigType::ct_pk:
    case ConfigType::ct_index_accompany:
    case ConfigType::ct_summary_accompany:
        return indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR;
    case ConfigType::ct_unknown:
    default:
        break;
    }
    assert(false);
    static std::string UNKNOW = "unknow";
    return UNKNOW;
}

const std::string& AttributeConfig::GetIndexName() const { return GetAttrName(); }

const std::string& AttributeConfig::GetAttrName() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldName();
}

std::vector<std::string> AttributeConfig::GetIndexPath() const
{
    std::string path;
    auto configType = _impl->configType;
    if (configType == ConfigType::ct_section) {
        path = indexlib::index::INVERTED_INDEX_PATH;
    } else {
        path = indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR;
    }
    std::string sliceDir = GetSliceDir();
    if (!sliceDir.empty()) {
        return {path + "/" + GetIndexName() + "/" + GetSliceDir()};
    }
    return {path + "/" + GetIndexName()};
}

std::string AttributeConfig::GetSliceDir() const
{
    if (_impl->sliceCount > 1 && _impl->sliceIdx != -1) {
        return "slice_" + std::to_string(_impl->sliceIdx);
    }
    return "";
}
std::vector<std::shared_ptr<FieldConfig>> AttributeConfig::GetFieldConfigs() const { return {_impl->fieldConfig}; }

void AttributeConfig::Check() const
{
    // unimplemented
}

attrid_t AttributeConfig::GetAttrId() const { return _impl->attrId; }

indexlib::config::CompressTypeOption AttributeConfig::GetCompressType() const { return _impl->compressType; }

AttributeConfig::ConfigType AttributeConfig::GetConfigType() const { return _impl->configType; }

float AttributeConfig::GetDefragSlicePercent() { return (float)_impl->defragSlicePercent / 100; }

fieldid_t AttributeConfig::GetFieldId() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldId();
}

const std::shared_ptr<indexlib::config::FileCompressConfig>& AttributeConfig::GetFileCompressConfig() const
{
    return _impl->fileCompressConfig;
}

const std::shared_ptr<FileCompressConfigV2>& AttributeConfig::GetFileCompressConfigV2() const
{
    return _impl->fileCompressConfigV2;
}

PackAttributeConfig* AttributeConfig::GetPackAttributeConfig() const { return _impl->packAttrConfig; }

bool AttributeConfig::IsAttributeUpdatable() const { return _impl->updatable; }

bool AttributeConfig::IsMultiValue() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->IsMultiValue();
}

bool AttributeConfig::IsLengthFixed() const
{
    assert(_impl->fieldConfig);
    if (!IsMultiValue()) {
        if (_impl->fieldConfig->GetFieldType() == ft_string) {
            return GetFixedMultiValueCount() != -1;
        }
        return true;
    }

    if (GetFieldType() == ft_string) {
        return false;
    }
    return GetFixedMultiValueCount() != -1;
}

FieldType AttributeConfig::GetFieldType() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldType();
}

bool AttributeConfig::IsLoadPatchExpand() const
{
    if (IsMultiValue() && IsAttributeUpdatable()) {
        return true;
    }
    if (GetFieldType() == ft_string && IsAttributeUpdatable()) {
        return true;
    }
    if (_impl->packAttrConfig) {
        return true;
    }
    return false;
}

bool AttributeConfig::IsUniqEncode() const { return _impl->compressType.HasUniqEncodeCompress(); }

bool AttributeConfig::SupportNull() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->IsEnableNullField();
}

int32_t AttributeConfig::GetFixedMultiValueCount() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFixedMultiValueCount();
}

bool AttributeConfig::IsMultiString() const { return GetFieldType() == ft_string && IsMultiValue(); }

void AttributeConfig::SetUpdatable(bool isUpdatable) { _impl->updatable = isUpdatable; }

Status AttributeConfig::SetCompressType(const std::string& compressStr)
{
    return _impl->compressType.Init(compressStr);
}

void AttributeConfig::SetAttrId(attrid_t id) { _impl->attrId = id; }

void AttributeConfig::SetConfigType(AttributeConfig::ConfigType type) { _impl->configType = type; }

void AttributeConfig::SetDefragSlicePercent(uint64_t percent) { _impl->defragSlicePercent = percent; }

void AttributeConfig::SetFileCompressConfig(
    const std::shared_ptr<indexlib::config::FileCompressConfig>& fileCompressConfig)
{
    _impl->fileCompressConfig = fileCompressConfig;
}
void AttributeConfig::SetFileCompressConfigV2(const std::shared_ptr<FileCompressConfigV2>& fileCompressConfigV2)
{
    _impl->fileCompressConfigV2 = fileCompressConfigV2;
}

void AttributeConfig::SetPackAttributeConfig(PackAttributeConfig* packAttrConfig)
{
    _impl->packAttrConfig = packAttrConfig;
}

Status AttributeConfig::AssertEqual(const AttributeConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->attrId, other._impl->attrId, "attrId not equal");
    CHECK_CONFIG_EQUAL(_impl->configType, other._impl->configType, "configType not equal");
    // CONFIG_ASSERT_EQUAL(mOwnerOpId, other.mOwnerOpId, "mOwnerOpId not equal");

    if (_impl->fileCompressConfig && other._impl->fileCompressConfig) {
        auto status = _impl->fileCompressConfig->CheckEqual(*other._impl->fileCompressConfig);
        RETURN_IF_STATUS_ERROR(status, "file compress config not equal");
    } else {
        CHECK_CONFIG_EQUAL(_impl->fileCompressConfig, other._impl->fileCompressConfig,
                           "file compress config not equal");
    }

    // TODO(makuo.mnb) support pack attribute and customized config
    // if (mPackAttrConfig && other.mPackAttrConfig) {
    //     mPackAttrConfig->AssertEqual(*other.mPackAttrConfig);
    // } else if (mPackAttrConfig || other.mPackAttrConfig) {
    //     INDEXLIB_FATAL_ERROR(AssertEqual, "mPackAttrConfig is not equal");
    // }

    // for (size_t i = 0; i < mCustomizedConfigs.size(); i++) {
    //     mCustomizedConfigs[i]->AssertEqual(*other.mCustomizedConfigs[i]);
    // }
    return Status::OK();
}

std::vector<std::shared_ptr<AttributeConfig>> AttributeConfig::CreateSliceAttributeConfigs(int64_t sliceCount)
{
    std::vector<std::shared_ptr<AttributeConfig>> attrConfigs;
    for (int64_t i = 0; i < sliceCount; i++) {
        auto attrConfig = Clone();
        if (sliceCount > 1) {
            attrConfig->_impl->sliceIdx = i;
        }
        attrConfigs.push_back(attrConfig);
    }
    return attrConfigs;
}

std::shared_ptr<AttributeConfig> AttributeConfig::Clone()
{
    std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig(_impl->configType));
    attrConfig->Init(_impl->fieldConfig);

    attrConfig->_impl->attrId = _impl->attrId;
    attrConfig->_impl->compressType = _impl->compressType;
    attrConfig->_impl->defragSlicePercent = _impl->defragSlicePercent;
    attrConfig->_impl->fileCompressConfig = _impl->fileCompressConfig;
    attrConfig->_impl->fileCompressConfigV2 = _impl->fileCompressConfigV2;
    attrConfig->_impl->packAttrConfig = _impl->packAttrConfig;
    attrConfig->_impl->u32OffsetThreshold = _impl->u32OffsetThreshold;
    attrConfig->_impl->status = _impl->status;
    attrConfig->_impl->updatable = _impl->updatable;
    attrConfig->_impl->sliceCount = _impl->sliceCount;
    return attrConfig;
}

void AttributeConfig::TEST_ClearCompressType() { _impl->compressType.ClearCompressType(); }

uint64_t AttributeConfig::GetU32OffsetThreshold() const { return _impl->u32OffsetThreshold; }
void AttributeConfig::SetU32OffsetThreshold(uint64_t offsetThreshold) { _impl->u32OffsetThreshold = offsetThreshold; }
const std::shared_ptr<FieldConfig>& AttributeConfig::GetFieldConfig() const { return _impl->fieldConfig; }

uint32_t AttributeConfig::GetFixLenFieldSize() const
{
    if (GetFieldType() == ft_string) {
        assert(!IsMultiValue());
        return GetFixedMultiValueCount() * sizeof(char);
    }

    if (!IsMultiValue()) {
        // single value float
        auto compressType = GetCompressType();
        if (GetFieldType() == FieldType::ft_float) {
            return indexlib::config::CompressTypeOption::GetSingleValueCompressLen(compressType);
        }
        return indexlib::index::SizeOfFieldType(GetFieldType());
    }
    // multi value float
    if (GetFieldType() == FieldType::ft_float) {
        auto compressType = GetCompressType();
        return indexlib::config::CompressTypeOption::GetMultiValueCompressLen(compressType, GetFixedMultiValueCount());
    }
    return indexlib::index::SizeOfFieldType(GetFieldType()) * GetFixedMultiValueCount();
}

bool AttributeConfig::IsDisable() const { return _impl->status == indexlib::is_disable; }
bool AttributeConfig::IsDeleted() const { return _impl->status == indexlib::is_deleted; }
bool AttributeConfig::IsNormal() const { return _impl->status == indexlib::is_normal; }
indexlib::IndexStatus AttributeConfig::GetStatus() const { return _impl->status; }
void AttributeConfig::Disable()
{
    _impl->status = (_impl->status == indexlib::is_normal) ? indexlib::is_disable : _impl->status;
}
Status AttributeConfig::Delete()
{
    if (GetPackAttributeConfig() != NULL) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "delete attribute [%s] fail, which is in pack attribute",
                               GetAttrName().c_str());
    }
    _impl->status = indexlib::is_deleted;
    return Status::OK();
}

bool AttributeConfig::IsLegacyAttributeConfig() const { return false; }

Status AttributeConfig::CheckCompatible(const IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const AttributeConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to AttributeConfig failed");
    }
    auto toJsonString = [](const config::IIndexConfig* config) {
        autil::legacy::Jsonizable::JsonWrapper json;
        config->Serialize(json);
        return ToJsonString(json.GetMap());
    };
    const auto& jsonStr = toJsonString(this);
    const auto& jsonStrOther = toJsonString(typedOther);
    if (jsonStr != jsonStrOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "original config [%s] is not compatible with [%s]",
                               jsonStr.c_str(), jsonStrOther.c_str());
    }
    return Status::OK();
}

} // namespace indexlibv2::config

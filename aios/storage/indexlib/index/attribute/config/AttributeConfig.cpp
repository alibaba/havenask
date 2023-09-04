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
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeConfig);

struct AttributeConfig::Impl {
    std::shared_ptr<config::FieldConfig> fieldConfig;
    attrid_t attrId = INVALID_ATTRID;
    indexlib::config::CompressTypeOption compressType;
    uint64_t defragSlicePercent = index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT;
    std::shared_ptr<indexlib::config::FileCompressConfig> fileCompressConfig;
    std::shared_ptr<config::FileCompressConfigV2> fileCompressConfigV2;
    PackAttributeConfig* packAttrConfig = nullptr;
    uint64_t u32OffsetThreshold = index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX;
    indexlib::IndexStatus status = indexlib::is_normal;
    bool updatable = true; // need initialize by FieldConfig
    int64_t sliceCount = 1;
    // not jsonize, no sliceIdx means no slice
    int64_t sliceIdx = -1;
};

AttributeConfig::AttributeConfig() : _impl(std::make_unique<Impl>()) {}

AttributeConfig::~AttributeConfig() {}

static bool IsUpdatableSingleValue(const std::shared_ptr<config::FieldConfig>& fieldConfig)
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

Status AttributeConfig::Init(const std::shared_ptr<config::FieldConfig>& fieldConfig)
{
    assert(fieldConfig);
    assert(fieldConfig->GetFixedMultiValueCount() != 0);
    if (fieldConfig == nullptr) {
        return Status::InternalError("attribute config init failed. field config is nullptr");
    }
    if (fieldConfig->GetFixedMultiValueCount() == 0) {
        return Status::InternalError("attribute config [%s] init failed. fixed multi value count 0 is invalid.",
                                     fieldConfig->GetFieldName().c_str());
    }

    _impl->fieldConfig = fieldConfig;
    _impl->updatable = IsUpdatableSingleValue(fieldConfig); // set DEFAULT, may changed by SetUpdatable()
    return Status::OK();
}

int64_t AttributeConfig::GetSliceCount() const { return _impl->sliceCount; }
int64_t AttributeConfig::GetSliceIdx() const { return _impl->sliceIdx; }

void AttributeConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                  const config::IndexConfigDeserializeResource& resource)
{
    // do not support ["attr1", "attr2"] in new schema no longer

    SetAttrId(idxInJsonArray);
    auto json = autil::legacy::Jsonizable::JsonWrapper(any);
    // field_name
    std::string fieldName;
    json.Jsonize(config::FieldConfig::FIELD_NAME, fieldName, fieldName);
    if (!_impl->fieldConfig) {
        auto status = Init(resource.GetFieldConfig(fieldName)); // indexlib v2
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(Schema, "init attr config [%s] failed, invalid field config, status[%s]",
                                 fieldName.c_str(), status.ToString().c_str());
        }
    } else {
        // already Init by legacy indexlib::AttributeConfig::SetFieldConfig
    }
    // compress_type
    std::string compressType;
    json.Jsonize(index::COMPRESS_TYPE, compressType, compressType);
    if (auto status = _impl->compressType.Init(compressType); !status.IsOK()) {
        INDEXLIB_FATAL_ERROR(Schema, "invalid compressType [%s], status[%s]", compressType.c_str(),
                             status.ToString().c_str());
    }

    // file_compressor
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
    // u32offset_threshold
    json.Jsonize(index::ATTRIBUTE_U32OFFSET_THRESHOLD, _impl->u32OffsetThreshold, _impl->u32OffsetThreshold);
    // defrag_slice_percent
    json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, _impl->defragSlicePercent, _impl->defragSlicePercent);
    // updatable
    json.Jsonize(index::ATTRIBUTE_UPDATABLE, _impl->updatable, _impl->updatable);
    // slice_count
    json.Jsonize(index::ATTRIBUTE_SLICE_COUNT, _impl->sliceCount, _impl->sliceCount);
}

void AttributeConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    // field_name
    json.Jsonize(config::FieldConfig::FIELD_NAME, GetAttrName());
    // file_compress or file_compressor
    if (_impl->fileCompressConfig) {
        std::string compressName = _impl->fileCompressConfig->GetCompressName();
        json.Jsonize(FILE_COMPRESS, compressName);
    } else if (_impl->fileCompressConfigV2) {
        std::string fileCompressor = _impl->fileCompressConfigV2->GetCompressName();
        json.Jsonize(index::FILE_COMPRESSOR, fileCompressor);
    }
    // compress_type
    if (_impl->compressType.HasCompressOption()) {
        std::string compressStr = _impl->compressType.GetCompressStr();
        assert(!compressStr.empty());
        json.Jsonize(index::COMPRESS_TYPE, compressStr);
    }
    // defrag_slice_percent
    if (_impl->defragSlicePercent != index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT) {
        json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, _impl->defragSlicePercent);
    }
    // u32offset_threshold
    if (_impl->u32OffsetThreshold != index::ATTRIBUTE_U32OFFSET_THRESHOLD_MAX) {
        json.Jsonize(index::ATTRIBUTE_U32OFFSET_THRESHOLD, _impl->u32OffsetThreshold);
    }
    // updatable
    json.Jsonize(index::ATTRIBUTE_UPDATABLE, _impl->updatable);
    // slice_count
    if (_impl->sliceCount > 1) {
        json.Jsonize(index::ATTRIBUTE_SLICE_COUNT, _impl->sliceCount);
    }
}

const std::string& AttributeConfig::GetIndexType() const { return ATTRIBUTE_INDEX_TYPE_STR; }

const std::string& AttributeConfig::GetIndexName() const { return GetAttrName(); }

const std::string& AttributeConfig::GetAttrName() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldName();
}

const std::string& AttributeConfig::GetIndexCommonPath() const { return ATTRIBUTE_INDEX_PATH; }

std::vector<std::string> AttributeConfig::GetIndexPath() const
{
    return {indexlib::util::PathUtil::JoinPath(GetIndexCommonPath(), GetIndexName(), GetSliceDir())};
}

std::string AttributeConfig::GetSliceDir() const
{
    if (_impl->sliceCount > 1 && _impl->sliceIdx != -1) {
        return "slice_" + std::to_string(_impl->sliceIdx);
    }
    return "";
}
std::vector<std::shared_ptr<config::FieldConfig>> AttributeConfig::GetFieldConfigs() const
{
    return {_impl->fieldConfig};
}

void AttributeConfig::Check() const
{
    CheckFieldType();
    CheckUniqEncode();
    CheckEquivalentCompress();
    CheckBlockFpEncode();
    // not support
    // CheckFp16Encode();
    // CheckFloatInt8Encode();
    // CheckUserDefineNullAttrValue();
}

void AttributeConfig::CheckUniqEncode() const
{
    if (GetCompressType().HasUniqEncodeCompress()) {
        if (IsMultiValue() || GetFieldType() == ft_string) {
            // do nothing
        } else {
            INDEXLIB_FATAL_ERROR(Schema, "uniqEncode invalid for field %s", GetIndexName().c_str());
        }
    }
}

void AttributeConfig::CheckFieldType() const
{
    if (GetFieldType() == ft_text) {
        INDEXLIB_FATAL_ERROR(Schema, "attribute not support text field type, fieldName[%s]", GetIndexName().c_str());
    }
}

void AttributeConfig::CheckEquivalentCompress() const
{
    // only not equal type allows to use compress
    if (GetCompressType().HasEquivalentCompress()) {
        if (!IsMultiValue() && GetFieldType() != ft_string && SupportNull()) {
            INDEXLIB_FATAL_ERROR(Schema,
                                 "equivalent compress invalid for field [%s], "
                                 "not support null with equal compress ",
                                 GetIndexName().c_str());
        }

        if (IsMultiValue() || GetFieldType() == ft_integer || GetFieldType() == ft_uint32 ||
            GetFieldType() == ft_long || GetFieldType() == ft_uint64 || GetFieldType() == ft_int8 ||
            GetFieldType() == ft_uint8 || GetFieldType() == ft_int16 || GetFieldType() == ft_uint16 ||
            GetFieldType() == ft_float || GetFieldType() == ft_fp8 || GetFieldType() == ft_fp16 ||
            GetFieldType() == ft_double || GetFieldType() == ft_string) {
            // do nothing
        } else {
            INDEXLIB_FATAL_ERROR(Schema, "equivalent compress invalid for field [%s]", GetIndexName().c_str());
        }
    }
}

void AttributeConfig::CheckBlockFpEncode() const
{
    if (!GetCompressType().HasBlockFpEncodeCompress()) {
        return;
    }
    if (!IsMultiValue() || GetFieldType() != ft_float || GetFixedMultiValueCount() == -1) {
        INDEXLIB_FATAL_ERROR(Schema, "BlockFpEncode invalid for field %s", GetIndexName().c_str());
    }
}

// void AttributeConfig::CheckFp16Encode() const
// {
//     if (GetFieldType() == ft_fp16) {
//         // fieldType set to ft_fp16 means already checked
//         return;
//     }

//     if (!GetCompressType().HasFp16EncodeCompress()) {
//         return;
//     }
//     if (GetFieldType() != ft_float) {
//         stringstream ss;
//         ss << "Fp16Encode invalid for field " << GetFieldName();
//         INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
//     }
//     if (!IsMultiValue() && GetCompressType().HasEquivalentCompress()) {
//         INDEXLIB_FATAL_ERROR(Schema, "can not use fp16 & equalCompress for field[%s] at the same time",
//                              GetFieldName().c_str());
//     }
// }

// void AttributeConfig::CheckFloatInt8Encode() const
// {
//     if (GetFieldType() == ft_fp8) {
//         return;
//     }

//     if (!GetCompressType().HasInt8EncodeCompress()) {
//         return;
//     }
//     if (GetFieldType() != ft_float) {
//         stringstream ss;
//         ss << "Int8FloatEncode invalid for field " << GetFieldName();
//         INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
//     }

//     if (!IsMultiValue() && GetCompressType().HasEquivalentCompress()) {
//         INDEXLIB_FATAL_ERROR(Schema, "can not use int8Compress & equalCompress for field[%s] at the same time",
//                              GetFieldName().c_str());
//     }
// }

attrid_t AttributeConfig::GetAttrId() const { return _impl->attrId; }

const indexlib::config::CompressTypeOption& AttributeConfig::GetCompressType() const { return _impl->compressType; }

uint64_t AttributeConfig::GetDefragSlicePercent() const { return _impl->defragSlicePercent; }

fieldid_t AttributeConfig::GetFieldId() const
{
    assert(_impl->fieldConfig);
    return _impl->fieldConfig->GetFieldId();
}

const std::shared_ptr<indexlib::config::FileCompressConfig>& AttributeConfig::GetFileCompressConfig() const
{
    return _impl->fileCompressConfig;
}

const std::shared_ptr<config::FileCompressConfigV2>& AttributeConfig::GetFileCompressConfigV2() const
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

void AttributeConfig::SetUpdatable(bool updatable) { _impl->updatable = updatable; }

Status AttributeConfig::SetCompressType(const std::string& compressStr)
{
    return _impl->compressType.Init(compressStr);
}

void AttributeConfig::SetAttrId(attrid_t id) { _impl->attrId = id; }

void AttributeConfig::SetDefragSlicePercent(uint64_t percent) { _impl->defragSlicePercent = percent; }

void AttributeConfig::SetFileCompressConfig(
    const std::shared_ptr<indexlib::config::FileCompressConfig>& fileCompressConfig)
{
    _impl->fileCompressConfig = fileCompressConfig;
}
void AttributeConfig::SetFileCompressConfigV2(const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfigV2)
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
    auto attrConfig = std::make_shared<AttributeConfig>();
    [[maybe_unused]] auto status = attrConfig->Init(_impl->fieldConfig);
    assert(status.IsOK());
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
const std::shared_ptr<config::FieldConfig>& AttributeConfig::GetFieldConfig() const { return _impl->fieldConfig; }

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

Status AttributeConfig::CheckCompatible(const config::IIndexConfig* other) const
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

bool AttributeConfig::IsDisabled() const { return _impl->status == indexlib::is_disable; }

} // namespace indexlibv2::index

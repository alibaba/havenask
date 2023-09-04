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
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeConfig);

struct PackAttributeConfig::Impl {
    std::string packName;
    indexlib::config::CompressTypeOption compressType;
    packattrid_t packAttrId = INVALID_PACK_ATTRID;
    std::vector<bool> isFromJsonMap;
    std::vector<std::shared_ptr<AttributeConfig>> attributeConfigVec;
    std::shared_ptr<indexlib::config::FileCompressConfig> fileCompressConfig;
    std::shared_ptr<config::FileCompressConfigV2> fileCompressConfigV2;
    uint64_t defragSlicePercent = index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT;
    PackFormat valueFormat = PF_DEFAULT;
    std::shared_ptr<AttributeConfig> disguisedAttributeConfig;
    // int64_t sliceCount = 1; // TODO: support
    bool updatable = false;
    bool isDisabled = false;
};

PackAttributeConfig::PackAttributeConfig() : _impl(std::make_unique<Impl>()) {}
PackAttributeConfig::~PackAttributeConfig() {}

void PackAttributeConfig::Deserialize(const autil::legacy::Any& any, size_t idxInJsonArray,
                                      const config::IndexConfigDeserializeResource& resource)
{
    SetPackAttrId(idxInJsonArray);
    auto json = autil::legacy::Jsonizable::JsonWrapper(any);
    // pack_name
    json.Jsonize(PACK_NAME, _impl->packName, _impl->packName);
    // sub_attributes
    assert(_impl->attributeConfigVec.empty());

    std::vector<autil::legacy::Any> subAttrs;
    json.Jsonize(SUB_ATTRIBUTES, subAttrs, subAttrs);
    if (subAttrs.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "packName[%s] has invalid subAttributes, is empty", _impl->packName.c_str());
    }
    size_t subAttrsIdx = 0;
    for (const auto& attr : subAttrs) {
        auto attributeConfig = std::make_shared<AttributeConfig>();
        bool isFromJsonMap = false;
        if (attr.GetType() == typeid(autil::legacy::json::JsonMap)) {
            isFromJsonMap = true;
            attributeConfig->Deserialize(attr, subAttrsIdx, resource);
        } else {
            std::string fieldName = autil::legacy::AnyCast<std::string>(attr);
            auto status = attributeConfig->Init(resource.GetFieldConfig(fieldName));
            if (!status.IsOK()) {
                INDEXLIB_FATAL_ERROR(Schema, "packName[%s] attribute config init failed [%s], status[%s]",
                                     _impl->packName.c_str(), fieldName.c_str(), status.ToString().c_str());
            }
        }
        auto status = AddAttributeConfig(attributeConfig, isFromJsonMap);
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(Schema, "packName[%s] has invalid subAttributes [%s], status[%s]",
                                 _impl->packName.c_str(), autil::legacy::ToJsonString(attr, true).c_str(),
                                 status.ToString().c_str());
        }
        subAttrsIdx++;
    }
    // value_format
    std::string valueFormat;
    json.Jsonize(VALUE_FORMAT, valueFormat, valueFormat);
    if (valueFormat == VALUE_FORMAT_IMPACT) {
        _impl->valueFormat = PF_IMPACT;
    } else if (valueFormat == VALUE_FORMAT_PLAIN) {
        _impl->valueFormat = PF_PLAIN;
    } else if (!valueFormat.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "packName[%s] has invalid valueFormat [%s]", _impl->packName.c_str(),
                             valueFormat.c_str());
    }
    // defrag_slice_percent
    json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, _impl->defragSlicePercent, _impl->defragSlicePercent);
    // compress_type
    std::string compressTypeStr;
    json.Jsonize(index::COMPRESS_TYPE, compressTypeStr, compressTypeStr);
    if (auto status = SetCompressType(compressTypeStr); !status.IsOK()) {
        INDEXLIB_FATAL_ERROR(Schema, "packName[%s] has invalid compressType [%s], status[%s]", _impl->packName.c_str(),
                             compressTypeStr.c_str(), status.ToString().c_str());
    }
    // file_compressor
    std::string fileCompressor;
    json.Jsonize(index::FILE_COMPRESSOR, fileCompressor, fileCompressor);
    if (!fileCompressor.empty()) {
        auto fileCompressConfig = resource.GetFileCompressConfig(fileCompressor);
        if (!fileCompressConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "packName[%s] get file compress[%s] failed", _impl->packName.c_str(),
                                 fileCompressor.c_str());
        }
        _impl->fileCompressConfigV2 = fileCompressConfig;
    }
    // updatable
    json.Jsonize(index::ATTRIBUTE_UPDATABLE, _impl->updatable, _impl->updatable);
    if (_impl->updatable) {
        INDEXLIB_FATAL_ERROR(Schema, "packName[%s] need update, but it is not supported currently",
                             _impl->packName.c_str());
    }
    // init
    CreateAttributeConfig();
}

void PackAttributeConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    // pack_name
    json.Jsonize(PACK_NAME, _impl->packName);
    // sub_attributes
    std::vector<autil::legacy::Any> subAttrs;
    assert(_impl->attributeConfigVec.size() == _impl->isFromJsonMap.size());
    for (size_t i = 0; i < _impl->attributeConfigVec.size(); i++) {
        const auto& config = _impl->attributeConfigVec[i];
        if (_impl->isFromJsonMap[i]) {
            autil::legacy::Jsonizable::JsonWrapper jsonWrapper;
            config->Serialize(jsonWrapper);
            subAttrs.push_back(jsonWrapper.GetMap());
        } else {
            subAttrs.push_back(config->GetAttrName());
        }
    }
    json.Jsonize(SUB_ATTRIBUTES, subAttrs);
    // compress_type
    if (std::string compressStr = _impl->compressType.GetCompressStr(); !compressStr.empty()) {
        json.Jsonize(index::COMPRESS_TYPE, compressStr);
    }
    // defrag_slice_percent
    if (_impl->defragSlicePercent != index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT) {
        json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, _impl->defragSlicePercent);
    }
    // file_compress or file_compressor
    if (_impl->fileCompressConfig) {
        std::string compressName = _impl->fileCompressConfig->GetCompressName();
        json.Jsonize(FILE_COMPRESS, compressName);
    } else if (_impl->fileCompressConfigV2) {
        std::string fileCompressor = _impl->fileCompressConfigV2->GetCompressName();
        json.Jsonize(index::FILE_COMPRESSOR, fileCompressor);
    }
    // value_format
    if (_impl->valueFormat != PF_DEFAULT) {
        std::string valueFormat;
        if (_impl->valueFormat == PF_IMPACT) {
            valueFormat = VALUE_FORMAT_IMPACT;
        } else if (_impl->valueFormat == PF_PLAIN) {
            valueFormat = VALUE_FORMAT_PLAIN;
        }
        json.Jsonize(VALUE_FORMAT, valueFormat);
    }
    // updatable
    json.Jsonize(index::ATTRIBUTE_UPDATABLE, _impl->updatable);
}

const std::string& PackAttributeConfig::GetPackName() const { return _impl->packName; }

indexlib::config::CompressTypeOption PackAttributeConfig::GetCompressType() const { return _impl->compressType; }

std::shared_ptr<indexlib::config::FileCompressConfig> PackAttributeConfig::GetFileCompressConfig() const
{
    return _impl->fileCompressConfig;
}
const std::shared_ptr<config::FileCompressConfigV2>& PackAttributeConfig::GetFileCompressConfigV2() const
{
    return _impl->fileCompressConfigV2;
}
packattrid_t PackAttributeConfig::GetPackAttrId() const { return _impl->packAttrId; }

void PackAttributeConfig::SetPackName(const std::string& packName) { _impl->packName = packName; }
void PackAttributeConfig::SetCompressType(const indexlib::config::CompressTypeOption& compressType)
{
    _impl->compressType = compressType;
}
Status PackAttributeConfig::SetCompressType(const std::string& compressStr)
{
    return _impl->compressType.Init(compressStr);
}
void PackAttributeConfig::SetPackAttrId(packattrid_t packAttrId) { _impl->packAttrId = packAttrId; }
void PackAttributeConfig::SetUpdatable(bool updatable) { _impl->updatable = updatable; }
Status PackAttributeConfig::AddAttributeConfig(const std::shared_ptr<AttributeConfig>& attributeConfig)
{
    return AddAttributeConfig(attributeConfig, false);
}
Status PackAttributeConfig::AddAttributeConfig(const std::shared_ptr<AttributeConfig>& attributeConfig,
                                               bool isFromJsonMap)
{
    if (_impl->packName == attributeConfig->GetAttrName()) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "sub attribute name is the same as pack attribute name[%s]",
                               _impl->packName.c_str());
    }
    if (attributeConfig->GetAttrId() == INVALID_ATTRID) {
        attributeConfig->SetAttrId(_impl->attributeConfigVec.size()); // set a temp id
    }
    _impl->attributeConfigVec.push_back(attributeConfig);
    _impl->isFromJsonMap.push_back(isFromJsonMap);
    attributeConfig->SetPackAttributeConfig(this);
    return Status::OK();
}
void PackAttributeConfig::SetDefragSlicePercent(uint64_t defragSlicePercent)
{
    _impl->defragSlicePercent = defragSlicePercent;
}
void PackAttributeConfig::SetFileCompressConfig(
    const std::shared_ptr<indexlib::config::FileCompressConfig>& fileCompressConfig)
{
    _impl->fileCompressConfig = fileCompressConfig;
}

void PackAttributeConfig::SetFileCompressConfigV2(
    const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfigV2)
{
    _impl->fileCompressConfigV2 = fileCompressConfigV2;
}

const std::vector<std::shared_ptr<AttributeConfig>>& PackAttributeConfig::GetAttributeConfigVec() const
{
    return _impl->attributeConfigVec;
}

void PackAttributeConfig::GetSubAttributeNames(std::vector<std::string>& fieldNames) const
{
    fieldNames.clear();
    fieldNames.reserve(_impl->attributeConfigVec.size());
    for (size_t i = 0; i < _impl->attributeConfigVec.size(); ++i) {
        fieldNames.push_back(_impl->attributeConfigVec[i]->GetAttrName());
    }
}

Status PackAttributeConfig::CheckEqual(const PackAttributeConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->packName, other._impl->packName, "packName not equal");
    CHECK_CONFIG_EQUAL(_impl->compressType, other._impl->compressType, "compressType not equal");
    CHECK_CONFIG_EQUAL(_impl->packAttrId, other._impl->packAttrId, "packAttrId not equal");
    CHECK_CONFIG_EQUAL(_impl->updatable, other._impl->updatable, "updatable not equal");
    CHECK_CONFIG_EQUAL(_impl->defragSlicePercent, other._impl->defragSlicePercent, "defragSlicePercent not equal");
    CHECK_CONFIG_EQUAL(_impl->isDisabled, other._impl->isDisabled, "isDisabled not equal");
    CHECK_CONFIG_EQUAL(_impl->valueFormat, other._impl->valueFormat, "valueFormat not equal");
    if (_impl->fileCompressConfig && other._impl->fileCompressConfig) {
        auto status = _impl->fileCompressConfig->CheckEqual(*other._impl->fileCompressConfig);
        RETURN_IF_STATUS_ERROR(status, "file compress config not equal");
    } else {
        CHECK_CONFIG_EQUAL(_impl->fileCompressConfig, other._impl->fileCompressConfig,
                           "file compress config not equal");
    }

    std::vector<std::string> subAttrNames;
    std::vector<std::string> otherSubAttrNames;
    GetSubAttributeNames(subAttrNames);
    other.GetSubAttributeNames(otherSubAttrNames);
    CHECK_CONFIG_EQUAL(subAttrNames, otherSubAttrNames, "sub attributes not equal");
    return Status::OK();
}

std::shared_ptr<AttributeConfig> PackAttributeConfig::CreateAttributeConfig() const
{
    class AttributeConfig4Pack final : public AttributeConfig
    {
        // insert indexer into segment with PackAttributeConfig::GetIndexType(), but get indexer from segment with
        // AttributeConfig4Pack::GetIndexType()
        const std::string& GetIndexType() const override { return indexlibv2::index::PACK_ATTRIBUTE_INDEX_TYPE_STR; }
        const std::string& GetIndexCommonPath() const override { return indexlibv2::index::PACK_ATTRIBUTE_INDEX_PATH; }
    };

    if (_impl->disguisedAttributeConfig) {
        return _impl->disguisedAttributeConfig;
    }

    assert(!IsDisabled());
    std::shared_ptr<config::FieldConfig> fieldConfig(new config::FieldConfig(_impl->packName, ft_string, false));
    fieldConfig->SetVirtual(true);
    auto attrConfig = std::make_shared<AttributeConfig4Pack>();
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create attribute [%s] config failed, status[%s]", _impl->packName.c_str(),
                  status.ToString().c_str());
        assert(false);
        return nullptr;
    }
    attrConfig->SetUpdatable(_impl->updatable);
    attrConfig->SetFileCompressConfig(_impl->fileCompressConfig);
    attrConfig->SetFileCompressConfigV2(_impl->fileCompressConfigV2);
    status = attrConfig->SetCompressType(_impl->compressType.GetCompressStr());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create attribute [%s] config failed, status[%s]", _impl->packName.c_str(),
                  status.ToString().c_str());
        assert(false);
        return nullptr;
    }
    attrConfig->SetDefragSlicePercent(_impl->defragSlicePercent);
    _impl->disguisedAttributeConfig = attrConfig;
    return attrConfig;
}

bool PackAttributeConfig::IsPackAttributeUpdatable() const { return _impl->updatable; }

void PackAttributeConfig::Disable()
{
    _impl->isDisabled = true;
    for (auto& attrConfig : _impl->attributeConfigVec) {
        attrConfig->Disable();
    }
}

bool PackAttributeConfig::SupportNull() const
{
    for (auto& attrConfig : _impl->attributeConfigVec) {
        if (attrConfig->SupportNull()) {
            return true;
        }
    }
    return false;
}

void PackAttributeConfig::EnableImpact() { _impl->valueFormat = PF_IMPACT; }
bool PackAttributeConfig::HasEnableImpact() const { return _impl->valueFormat == PF_IMPACT; }

bool PackAttributeConfig::HasEnablePlainFormat() const { return _impl->valueFormat == PF_PLAIN; }
void PackAttributeConfig::EnablePlainFormat() { _impl->valueFormat = PF_PLAIN; }

uint64_t PackAttributeConfig::GetDefragSlicePercent() const { return _impl->defragSlicePercent; }

const std::string& PackAttributeConfig::GetIndexType() const
{
    return indexlibv2::index::PACK_ATTRIBUTE_INDEX_TYPE_STR;
}
const std::string& PackAttributeConfig::GetIndexName() const { return GetPackName(); }
const std::string& PackAttributeConfig::GetIndexCommonPath() const
{
    return indexlibv2::index::PACK_ATTRIBUTE_INDEX_PATH;
}
std::vector<std::string> PackAttributeConfig::GetIndexPath() const
{
    return {GetIndexCommonPath() + "/" + GetIndexName()};
}
// std::string PackAttributeConfig::GetPatchPath(const std::string& subAttrName) const
// {
//     return indexlibv2::index::PACK_ATTRIBUTE_INDEX_PATH + "/" + GetIndexName() + "/" + subAttrName;
// }

std::vector<std::shared_ptr<config::FieldConfig>> PackAttributeConfig::GetFieldConfigs() const
{
    std::vector<std::shared_ptr<config::FieldConfig>> ret;
    for (const auto& attrConfig : _impl->attributeConfigVec) {
        ret.push_back(attrConfig->GetFieldConfig());
    }
    return ret;
}

void PackAttributeConfig::Check() const {}

Status PackAttributeConfig::CheckCompatible(const config::IIndexConfig* other) const
{
    const auto* typedOther = dynamic_cast<const PackAttributeConfig*>(other);
    if (!typedOther) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(), "cast to PackAttributeConfig failed");
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
bool PackAttributeConfig::IsDisabled() const { return _impl->isDisabled; }

} // namespace indexlibv2::index

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
#include "indexlib/index/attribute/config/PackAttributeConfig.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
using namespace std;
using indexlib::config::CompressTypeOption;
using indexlib::config::FileCompressConfig;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeConfig);

struct PackAttributeConfig::Impl {
    std::string packAttrName;
    CompressTypeOption compressType;
    packattrid_t packId = INVALID_PACK_ATTRID;
    std::vector<std::shared_ptr<AttributeConfig>> attributeConfigVec;
    std::shared_ptr<FileCompressConfig> fileCompressConfig;
    uint64_t defragSlicePercent;
    PackFormat valueFormat = PF_DEFAULT;
    bool isUpdatable = false;
    bool isDisable = false;
    Impl(const std::string& attrName, const CompressTypeOption& compressType, uint64_t defragSlicePercent,
         const std::shared_ptr<FileCompressConfig>& fileCompressConfig)
        : packAttrName(attrName)
        , compressType(compressType)
        , fileCompressConfig(fileCompressConfig)
        , defragSlicePercent(defragSlicePercent)
    {
    }
};

PackAttributeConfig::PackAttributeConfig(const string& attrName, const CompressTypeOption& compressType,
                                         uint64_t defragSlicePercent,
                                         const std::shared_ptr<FileCompressConfig>& fileCompressConfig)
    : _impl(make_unique<Impl>(attrName, compressType, defragSlicePercent, fileCompressConfig))
{
    assert(attrName.size() > 0);
}

PackAttributeConfig::~PackAttributeConfig() {}

void PackAttributeConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    json.Jsonize(PACK_ATTR_NAME_FIELD, _impl->packAttrName);
    vector<string> subAttrNames;
    GetSubAttributeNames(subAttrNames);
    json.Jsonize(PACK_ATTR_SUB_ATTR_FIELD, subAttrNames);

    string compressStr = _impl->compressType.GetCompressStr();
    json.Jsonize(index::COMPRESS_TYPE, compressStr);
    if (_impl->defragSlicePercent != index::ATTRIBUTE_DEFAULT_DEFRAG_SLICE_PERCENT) {
        json.Jsonize(index::ATTRIBUTE_DEFRAG_SLICE_PERCENT, _impl->defragSlicePercent);
    }
    if (_impl->fileCompressConfig) {
        string compressName = _impl->fileCompressConfig->GetCompressName();
        json.Jsonize(FILE_COMPRESS, compressName);
    }
    if (_impl->valueFormat != PF_DEFAULT) {
        string valueFormat;
        if (_impl->valueFormat == PF_IMPACT) {
            valueFormat = PACK_ATTR_VALUE_FORMAT_IMPACT;
        } else if (_impl->valueFormat == PF_PLAIN) {
            valueFormat = PACK_ATTR_VALUE_FORMAT_PLAIN;
        }
        json.Jsonize(PACK_ATTR_VALUE_FORMAT, valueFormat);
    }
}

const string& PackAttributeConfig::GetAttrName() const { return _impl->packAttrName; }

CompressTypeOption PackAttributeConfig::GetCompressType() const { return _impl->compressType; }

std::shared_ptr<FileCompressConfig> PackAttributeConfig::GetFileCompressConfig() const
{
    return _impl->fileCompressConfig;
}
packattrid_t PackAttributeConfig::GetPackAttrId() const { return _impl->packId; }
void PackAttributeConfig::SetPackAttrId(packattrid_t packId) { _impl->packId = packId; }

Status PackAttributeConfig::AddAttributeConfig(const std::shared_ptr<AttributeConfig>& attributeConfig)
{
    if (_impl->packAttrName == attributeConfig->GetAttrName()) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "sub attribute name is the same as pack attribute name[%s]!",
                               _impl->packAttrName.c_str());
    }

    _impl->attributeConfigVec.push_back(attributeConfig);
    _impl->isUpdatable = _impl->isUpdatable || attributeConfig->IsAttributeUpdatable();
    attributeConfig->SetPackAttributeConfig(this);
    return Status::OK();
}

const vector<std::shared_ptr<AttributeConfig>>& PackAttributeConfig::GetAttributeConfigVec() const
{
    return _impl->attributeConfigVec;
}

void PackAttributeConfig::GetSubAttributeNames(vector<string>& fieldNames) const
{
    fieldNames.clear();
    fieldNames.reserve(_impl->attributeConfigVec.size());
    for (size_t i = 0; i < _impl->attributeConfigVec.size(); ++i) {
        fieldNames.push_back(_impl->attributeConfigVec[i]->GetAttrName());
    }
}

Status PackAttributeConfig::CheckEqual(const PackAttributeConfig& other) const
{
    CHECK_CONFIG_EQUAL(_impl->packAttrName, other._impl->packAttrName, "packAttrName not equal");
    CHECK_CONFIG_EQUAL(_impl->compressType, other._impl->compressType, "compressType not equal");
    CHECK_CONFIG_EQUAL(_impl->packId, other._impl->packId, "packId not equal");
    CHECK_CONFIG_EQUAL(_impl->isUpdatable, other._impl->isUpdatable, "isUpdatable not equal");
    CHECK_CONFIG_EQUAL(_impl->defragSlicePercent, other._impl->defragSlicePercent, "defragSlicePercent not equal");
    CHECK_CONFIG_EQUAL(_impl->isDisable, other._impl->isDisable, "isDisable not equal");
    CHECK_CONFIG_EQUAL(_impl->valueFormat, other._impl->valueFormat, "valueFormat not equal");
    if (_impl->fileCompressConfig && other._impl->fileCompressConfig) {
        auto status = _impl->fileCompressConfig->CheckEqual(*other._impl->fileCompressConfig);
        RETURN_IF_STATUS_ERROR(status, "file compress config not equal");
    } else {
        CHECK_CONFIG_EQUAL(_impl->fileCompressConfig, other._impl->fileCompressConfig,
                           "file compress config not equal");
    }

    vector<string> subAttrNames;
    vector<string> otherSubAttrNames;
    GetSubAttributeNames(subAttrNames);
    other.GetSubAttributeNames(otherSubAttrNames);
    CHECK_CONFIG_EQUAL(subAttrNames, otherSubAttrNames, "sub attributes not equal");
    return Status::OK();
}

std::shared_ptr<AttributeConfig> PackAttributeConfig::CreateAttributeConfig() const
{
    std::shared_ptr<FieldConfig> fieldConfig(new FieldConfig(_impl->packAttrName, ft_string, false));
    fieldConfig->SetVirtual(true);
    std::shared_ptr<AttributeConfig> attrConfig(new AttributeConfig(AttributeConfig::ct_normal));
    attrConfig->Init(fieldConfig);
    attrConfig->SetUpdatable(_impl->isUpdatable);
    attrConfig->SetFileCompressConfig(_impl->fileCompressConfig);
    // status would always be ok
    [[maybe_unused]] auto status = attrConfig->SetCompressType(_impl->compressType.GetCompressStr());
    assert(status.IsOK());
    attrConfig->SetDefragSlicePercent(_impl->defragSlicePercent);
    return attrConfig;
}

bool PackAttributeConfig::IsUpdatable() const { return _impl->isUpdatable; }

void PackAttributeConfig::Disable()
{
    _impl->isDisable = true;
    for (auto& attrConfig : _impl->attributeConfigVec) {
        attrConfig->Disable();
    }
}

bool PackAttributeConfig::IsDisable() const { return _impl->isDisable; }

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

const std::string& PackAttributeConfig::GetIndexType() const { return indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR; }
const std::string& PackAttributeConfig::GetIndexName() const { return GetAttrName(); }
std::vector<std::string> PackAttributeConfig::GetIndexPath() const
{
    return {indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR + "/" + GetIndexName()};
}

std::vector<std::shared_ptr<FieldConfig>> PackAttributeConfig::GetFieldConfigs() const
{
    std::vector<std::shared_ptr<FieldConfig>> ret;
    for (const auto& attrConfig : _impl->attributeConfigVec) {
        ret.push_back(attrConfig->GetFieldConfig());
    }
    return ret;
}

void PackAttributeConfig::Check() const {}

Status PackAttributeConfig::CheckCompatible(const IIndexConfig* other) const
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

} // namespace indexlibv2::config

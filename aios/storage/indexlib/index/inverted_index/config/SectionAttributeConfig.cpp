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
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"

#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/util/Exception.h"

using namespace std;
using indexlib::config::CompressTypeOption;
using indexlib::config::FileCompressConfig;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, SectionAttributeConfig);

struct SectionAttributeConfig::Impl {
    std::shared_ptr<FileCompressConfig> compressConfig;
    std::shared_ptr<FileCompressConfigV2> compressConfigV2;
    CompressTypeOption compressType;
    bool hasSectionWeight = true;
    bool hasFieldId = true;
    Impl() {}
    Impl(bool hasSectionWeight, bool hasFieldId) : hasSectionWeight(hasSectionWeight), hasFieldId(hasFieldId) {}
};

SectionAttributeConfig::SectionAttributeConfig() : _impl(make_unique<Impl>()) {}

SectionAttributeConfig::SectionAttributeConfig(const string& compressString, bool hasSectionWeight, bool hasFieldId)
    : _impl(make_unique<Impl>(hasSectionWeight, hasFieldId))
{
    auto status = _impl->compressType.Init(compressString);
    if (!status.IsOK()) {
        INDEXLIB_FATAL_ERROR(BadParameter, "CompressTypeOption init fail, [%s]", compressString.c_str());
    }
}

SectionAttributeConfig::~SectionAttributeConfig() {}

bool SectionAttributeConfig::IsUniqEncode() const { return _impl->compressType.HasUniqEncodeCompress(); }

bool SectionAttributeConfig::HasEquivalentCompress() const { return _impl->compressType.HasEquivalentCompress(); }

bool SectionAttributeConfig::HasSectionWeight() const { return _impl->hasSectionWeight; }

bool SectionAttributeConfig::HasFieldId() const { return _impl->hasFieldId; }

void SectionAttributeConfig::Jsonize(Jsonizable::JsonWrapper& json)
{
    json.Jsonize(HAS_SECTION_WEIGHT, _impl->hasSectionWeight, true);
    json.Jsonize(HAS_SECTION_FIELD_ID, _impl->hasFieldId, true);
    if (json.GetMode() == TO_JSON) {
        string compressStr = _impl->compressType.GetCompressStr();
        if (!compressStr.empty()) {
            json.Jsonize(index::COMPRESS_TYPE, compressStr);
        }
    } else {
        string compressStr;
        json.Jsonize(index::COMPRESS_TYPE, compressStr, compressStr);
        auto status = _impl->compressType.Init(compressStr);
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(BadParameter, "CompressTypeOption init fail, [%s]", compressStr.c_str());
        }
    }
}

Status SectionAttributeConfig::CheckEqual(const SectionAttributeConfig& other) const
{
    auto status = _impl->compressType.AssertEqual(other._impl->compressType);
    RETURN_IF_STATUS_ERROR(status, "compress type not equal");

    CHECK_CONFIG_EQUAL(_impl->hasSectionWeight, other._impl->hasSectionWeight, "has_section_weight not equal");

    CHECK_CONFIG_EQUAL(_impl->hasFieldId, other._impl->hasFieldId, "has_field_id not equal");
    return Status::OK();
}

bool SectionAttributeConfig::operator==(const SectionAttributeConfig& other) const
{
    return _impl->compressType == other._impl->compressType &&
           _impl->hasSectionWeight == other._impl->hasSectionWeight && _impl->hasFieldId == other._impl->hasFieldId;
}

bool SectionAttributeConfig::operator!=(const SectionAttributeConfig& other) const { return !(*this == other); }

std::shared_ptr<index::AttributeConfig> SectionAttributeConfig::CreateAttributeConfig(const string& indexName) const
{
    class AttributeConfig4Section final : public index::AttributeConfig
    {
        const std::string& GetIndexCommonPath() const override { return indexlib::index::INVERTED_INDEX_PATH; }
    };

    string fieldName = IndexNameToSectionAttributeName(indexName);
    // single string attribute
    shared_ptr<FieldConfig> fieldConfig(new FieldConfig(fieldName, ft_string, false));
    fieldConfig->SetVirtual(true);
    auto attrConfig = std::make_shared<AttributeConfig4Section>();
    auto status = attrConfig->Init(fieldConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "attribute config init failed, indexName %s", indexName.c_str());
        assert(false);
        return nullptr;
    }
    attrConfig->SetFileCompressConfig(_impl->compressConfig);
    attrConfig->SetFileCompressConfigV2(_impl->compressConfigV2);
    status = attrConfig->SetCompressType(_impl->compressType.GetCompressStr());
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "attribute config init failed, indexName %s", indexName.c_str());
        assert(false);
        return nullptr;
    }
    return attrConfig;
}

string SectionAttributeConfig::IndexNameToSectionAttributeName(const string& indexName)
{
    return indexName + SECTION_DIR_NAME_SUFFIX;
}

string SectionAttributeConfig::SectionAttributeNameToIndexName(const string& attrName)
{
    size_t suffixLen = SECTION_DIR_NAME_SUFFIX.length();
    if (attrName.size() <= suffixLen) {
        return "";
    }
    if (attrName.substr(attrName.size() - suffixLen) != SECTION_DIR_NAME_SUFFIX) {
        return "";
    }
    return attrName.substr(0, attrName.size() - suffixLen);
}

void SectionAttributeConfig::SetFileCompressConfig(const std::shared_ptr<FileCompressConfig>& compressConfig)
{
    _impl->compressConfig = compressConfig;
}

void SectionAttributeConfig::SetFileCompressConfigV2(const std::shared_ptr<FileCompressConfigV2>& compressConfigV2)
{
    _impl->compressConfigV2 = compressConfigV2;
}

std::shared_ptr<indexlib::config::FileCompressConfig> SectionAttributeConfig::GetFileCompressConfig() const
{
    return _impl->compressConfig;
}

std::shared_ptr<config::FileCompressConfigV2> SectionAttributeConfig::GetFileCompressConfigV2() const
{
    return _impl->compressConfigV2;
}
indexlib::config::CompressTypeOption SectionAttributeConfig::GetCompressType() const { return _impl->compressType; }

} // namespace indexlibv2::config

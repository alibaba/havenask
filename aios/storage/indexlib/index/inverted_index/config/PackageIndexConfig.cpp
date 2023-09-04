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
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using indexlib::config::FileCompressConfig;

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, PackageIndexConfig);

struct PackageIndexConfig::Impl {
    std::vector<int32_t> fieldBoosts;
    std::vector<indexlib::firstocc_t> fieldHits;
    std::vector<int32_t> fieldIdxInPack; // map fieldId => fieldIdxInPack
    std::vector<std::shared_ptr<FieldConfig>> indexFields;
    int32_t totalFieldBoost = 0;
    pos_t maxFirstOccInDoc = INVALID_POSITION;
    std::shared_ptr<SectionAttributeConfig> sectionAttributeConfig;
    bool hasSectionAttribute = true;
};

PackageIndexConfig::PackageIndexConfig(const string& indexName, InvertedIndexType indexType)
    : InvertedIndexConfig(indexName, indexType)
    , _impl(make_unique<Impl>())
{
    _impl->sectionAttributeConfig.reset(new SectionAttributeConfig);
}

PackageIndexConfig::PackageIndexConfig(const PackageIndexConfig& other)
    : InvertedIndexConfig(other)
    , _impl(make_unique<Impl>(*(other._impl)))
{
}

PackageIndexConfig::~PackageIndexConfig() {}

uint32_t PackageIndexConfig::GetFieldCount() const { return _impl->indexFields.size(); }
InvertedIndexConfig::Iterator PackageIndexConfig::DoCreateIterator() const
{
    return InvertedIndexConfig::Iterator(_impl->indexFields);
}

int32_t PackageIndexConfig::GetFieldBoost(fieldid_t id) const
{
    return (size_t)id < _impl->fieldBoosts.size() ? _impl->fieldBoosts[id] : 0;
}

void PackageIndexConfig::SetFieldBoost(fieldid_t id, int32_t boost)
{
    if (id + 1 > (fieldid_t)_impl->fieldBoosts.size()) {
        _impl->fieldBoosts.resize(id + 1, 0);
    }
    _impl->totalFieldBoost += boost - _impl->fieldBoosts[id];
    _impl->fieldBoosts[id] = boost;
}

int32_t PackageIndexConfig::GetFieldIdxInPack(fieldid_t id) const
{
    return (size_t)id < _impl->fieldIdxInPack.size() ? _impl->fieldIdxInPack[id] : -1;
}

int32_t PackageIndexConfig::GetFieldIdxInPack(const std::string& fieldName) const
{
    fieldid_t fid = INVALID_FIELDID;
    for (auto fieldConfig : _impl->indexFields) {
        if (fieldConfig->GetFieldName() == fieldName) {
            fid = fieldConfig->GetFieldId();
            break;
        }
    }
    if (fid == INVALID_FIELDID) {
        return -1;
    }
    return GetFieldIdxInPack(fid);
}

fieldid_t PackageIndexConfig::GetFieldId(int32_t fieldIdxInPack) const
{
    if (fieldIdxInPack >= 0 && fieldIdxInPack < (int32_t)_impl->indexFields.size()) {
        return _impl->indexFields[fieldIdxInPack]->GetFieldId();
    }
    return INVALID_FIELDID;
}

int32_t PackageIndexConfig::GetTotalFieldBoost() const { return _impl->totalFieldBoost; }

Status PackageIndexConfig::AddFieldConfig(const std::shared_ptr<FieldConfig>& fieldConfig, int32_t boost)
{
    assert(fieldConfig);
    assert(GetInvertedIndexType() == it_pack || GetInvertedIndexType() == it_expack ||
           GetInvertedIndexType() == it_customized);

    FieldType fieldType = fieldConfig->GetFieldType();
    if (!CheckFieldType(fieldType)) {
        stringstream ss;
        ss << "InvertedIndexType " << InvertedIndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType())
           << " and FieldType " << FieldConfig::FieldTypeToStr(fieldType) << " Mismatch.";
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "%s", ss.str().c_str());
    }

    uint32_t maxFieldNum = 0;
    switch (GetInvertedIndexType()) {
    case it_pack:
        maxFieldNum = PackageIndexConfig::PACK_MAX_FIELD_NUM;
        break;
    case it_expack:
        maxFieldNum = PackageIndexConfig::EXPACK_MAX_FIELD_NUM;
        break;
    case it_customized:
        maxFieldNum = PackageIndexConfig::CUSTOMIZED_MAX_FIELD_NUM;
        break;
    default:
        break;
    }

    if (_impl->indexFields.size() >= maxFieldNum) {
        stringstream ss;
        ss << "Only support at most " << maxFieldNum << " fields in package index";
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "%s", ss.str().c_str());
    }

    fieldid_t id = fieldConfig->GetFieldId();
    if (id < (fieldid_t)_impl->fieldHits.size() && _impl->fieldHits[id] != 0) {
        stringstream ss;
        ss << "duplicated field in package: " << GetIndexName();
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "%s", ss.str().c_str());
    }

    if (fieldConfig->IsEnableNullField()) {
        for (auto& field : _impl->indexFields) {
            if (field->IsEnableNullField() &&
                field->GetNullFieldLiteralString() != fieldConfig->GetNullFieldLiteralString()) {
                RETURN_IF_STATUS_ERROR(Status::ConfigError(),
                                       "enable null fields in package index [%s] "
                                       "with different null literal string.",
                                       GetIndexName().c_str());
            }
        }
    }
    _impl->indexFields.push_back(fieldConfig);
    SetFieldBoost(id, boost);

    if (id + 1 > (fieldid_t)_impl->fieldHits.size()) {
        _impl->fieldHits.resize(id + 1, (indexlib::firstocc_t)0);
    }
    _impl->fieldHits[id] = _impl->indexFields.size();

    if (id + 1 > (fieldid_t)_impl->fieldIdxInPack.size()) {
        _impl->fieldIdxInPack.resize(id + 1, (int32_t)-1);
    }
    _impl->fieldIdxInPack[id] = _impl->indexFields.size() - 1;
    return Status::OK();
}

bool PackageIndexConfig::HasSectionAttribute() const
{
    return _impl->hasSectionAttribute && (GetOptionFlag() & of_position_list);
}

const std::shared_ptr<SectionAttributeConfig>& PackageIndexConfig::GetSectionAttributeConfig() const
{
    return _impl->sectionAttributeConfig;
}

void PackageIndexConfig::SetHasSectionAttributeFlag(bool flag) { _impl->hasSectionAttribute = flag; }

const std::vector<std::shared_ptr<FieldConfig>>& PackageIndexConfig::GetFieldConfigVector() const
{
    return _impl->indexFields;
}

std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> PackageIndexConfig::GetFieldConfigs() const
{
    return _impl->indexFields;
}

bool PackageIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    if (fieldId < 0 || fieldId >= (fieldid_t)_impl->fieldIdxInPack.size()) {
        return false;
    }
    return _impl->fieldIdxInPack[fieldId] >= 0;
}
void PackageIndexConfig::SetMaxFirstOccInDoc(pos_t pos) { _impl->maxFirstOccInDoc = pos; }

pos_t PackageIndexConfig::GetMaxFirstOccInDoc() const { return _impl->maxFirstOccInDoc; }

void PackageIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    if (IsVirtual()) {
        return;
    }
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    InvertedIndexConfig::Serialize(json);

    if (!_impl->indexFields.empty()) {
        vector<JsonMap> indexFields;
        indexFields.reserve(_impl->indexFields.size());
        for (std::vector<std::shared_ptr<FieldConfig>>::iterator it = _impl->indexFields.begin();
             it != _impl->indexFields.end(); ++it) {
            JsonMap fieldBoostInfo;
            fieldBoostInfo[indexlibv2::config::FieldConfig::FIELD_NAME] = (*it)->GetFieldName();
            fieldBoostInfo[FIELD_BOOST] = GetFieldBoost((*it)->GetFieldId());
            indexFields.push_back(fieldBoostInfo);
        }
        json.Jsonize(INDEX_FIELDS, indexFields);
    }

    if (_impl->hasSectionAttribute) {
        SectionAttributeConfig defaultSecAttrConfig;
        if (_impl->sectionAttributeConfig && *_impl->sectionAttributeConfig != defaultSecAttrConfig) {
            json.Jsonize(SECTION_ATTRIBUTE_CONFIG, _impl->sectionAttributeConfig);
        }
    } else {
        json.Jsonize(HAS_SECTION_ATTRIBUTE, _impl->hasSectionAttribute);
    }

    if (_impl->maxFirstOccInDoc != INVALID_POSITION) {
        json.Jsonize(MAX_FIRSTOCC_IN_DOC, _impl->maxFirstOccInDoc);
    }
}

Status PackageIndexConfig::CheckEqual(const InvertedIndexConfig& other) const
{
    auto status = InvertedIndexConfig::CheckEqual(other);
    RETURN_IF_STATUS_ERROR(status, "InvertedIndexConfig not equal");
    const PackageIndexConfig& other2 = (const PackageIndexConfig&)other;
    CHECK_CONFIG_EQUAL(_impl->fieldBoosts, other2._impl->fieldBoosts, "fieldBoosts not equal");
    CHECK_CONFIG_EQUAL(_impl->totalFieldBoost, other2._impl->totalFieldBoost, "totalFieldBoost not equal");
    CHECK_CONFIG_EQUAL(_impl->indexFields.size(), other2._impl->indexFields.size(), "indexFields's size not equal");
    CHECK_CONFIG_EQUAL(_impl->hasSectionAttribute, other2._impl->hasSectionAttribute, "hasSectionAttribute not equal");
    CHECK_CONFIG_EQUAL(_impl->maxFirstOccInDoc, other2._impl->maxFirstOccInDoc, "maxFirstOccInDoc not equal");
    for (size_t i = 0; i < _impl->indexFields.size(); ++i) {
        auto status = _impl->indexFields[i]->CheckEqual(*(other2._impl->indexFields[i]));
        RETURN_IF_STATUS_ERROR(status, "field config [%s] not equal", _impl->indexFields[i]->GetFieldName().c_str());
    }

    CHECK_CONFIG_EQUAL((GetDictConfig() != NULL), (other2.GetDictConfig() != NULL),
                       "high frequency dictionary config is not equal");
    if (GetDictConfig()) {
        auto status = GetDictConfig()->CheckEqual(*other2.GetDictConfig());
        RETURN_IF_STATUS_ERROR(status, "dict config not equal");
    }
    CHECK_CONFIG_EQUAL(GetHighFrequencyTermPostingType(), other2.GetHighFrequencyTermPostingType(),
                       "high frequency term posting type is not equal");

    if (_impl->sectionAttributeConfig && other2._impl->sectionAttributeConfig) {
        auto status = _impl->sectionAttributeConfig->CheckEqual(*other2._impl->sectionAttributeConfig);
        RETURN_IF_STATUS_ERROR(status, "section attribute config not equal");
    } else if (_impl->sectionAttributeConfig || other2._impl->sectionAttributeConfig) {
        RETURN_IF_STATUS_ERROR(Status::ConfigError(), "section attribute config not equal");
    }
    return Status::OK();
}
InvertedIndexConfig* PackageIndexConfig::Clone() const { return new PackageIndexConfig(*this); }
Status PackageIndexConfig::SetDefaultAnalyzer()
{
    string analyzer;
    for (size_t i = 0; i < _impl->indexFields.size(); ++i) {
        if (analyzer.empty()) {
            analyzer = _impl->indexFields[i]->GetAnalyzerName();
            continue;
        }

        if (analyzer != _impl->indexFields[i]->GetAnalyzerName()) {
            stringstream ss;
            ss << "ambiguous analyzer name in pack index, index_name = " << GetIndexName()
               << ", analyzerName1 = " << analyzer << ", analyzerName2 = " << _impl->indexFields[i]->GetAnalyzerName();
            RETURN_IF_STATUS_ERROR(Status::ConfigError(), "%s", ss.str().c_str());
        }
    }
    SetAnalyzer(analyzer);
    return Status::OK();
}

// only for test
void PackageIndexConfig::SetSectionAttributeConfig(std::shared_ptr<SectionAttributeConfig> sectionAttributeConfig)
{
    _impl->sectionAttributeConfig = sectionAttributeConfig;
}

void PackageIndexConfig::Check() const
{
    InvertedIndexConfig::Check();
    CheckDictHashMustBeDefault();
}

bool PackageIndexConfig::CheckFieldType(FieldType ft) const { return ft == ft_text; }

indexlib::util::KeyValueMap PackageIndexConfig::GetDictHashParams() const { return {}; }

std::vector<std::string> PackageIndexConfig::GetIndexPath() const
{
    auto paths = InvertedIndexConfig::GetIndexPath();
    auto sectionAttributeConfig = GetSectionAttributeConfig();
    if (sectionAttributeConfig) {
        auto attributeConfig = sectionAttributeConfig->CreateAttributeConfig(GetIndexName());
        assert(attributeConfig);
        auto attrPaths = attributeConfig->GetIndexPath();
        paths.insert(paths.end(), attrPaths.begin(), attrPaths.end());
    }
    return paths;
}

void PackageIndexConfig::CheckDictHashMustBeDefault() const
{
    for (std::shared_ptr<FieldConfig> fieldConfig : _impl->indexFields) {
        indexlib::util::KeyValueMap params = indexlib::util::ConvertFromJsonMap(fieldConfig->GetUserDefinedParam());
        string hashFunc = indexlib::util::GetValueFromKeyValueMap(params, "dict_hash_func", "default");
        if (hashFunc != "default") {
            INDEXLIB_FATAL_ERROR(
                Schema, "index [%s] with type [%s], field [%s] not support undefault hash function [%s]",
                GetIndexName().c_str(), InvertedIndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType()),
                fieldConfig->GetFieldName().c_str(), hashFunc.c_str());
        }
    }
}

void PackageIndexConfig::SetFileCompressConfig(const std::shared_ptr<FileCompressConfig>& compressConfig)
{
    InvertedIndexConfig::SetFileCompressConfig(compressConfig);
    if (_impl->sectionAttributeConfig) {
        _impl->sectionAttributeConfig->SetFileCompressConfig(compressConfig);
    }
}

void PackageIndexConfig::SetFileCompressConfigV2(const std::shared_ptr<config::FileCompressConfigV2>& compressConfigV2)
{
    InvertedIndexConfig::SetFileCompressConfigV2(compressConfigV2);
    if (_impl->sectionAttributeConfig) {
        _impl->sectionAttributeConfig->SetFileCompressConfigV2(compressConfigV2);
    }
}

void PackageIndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                       const config::IndexConfigDeserializeResource& resource)
{
    InvertedIndexConfig::DoDeserialize(any, resource);
    autil::legacy::Jsonizable::JsonWrapper json(any);
    std::vector<std::string> fieldNames;
    std::vector<int32_t> boosts;
    autil::legacy::json::JsonArray indexFields;
    json.Jsonize(INDEX_FIELDS, indexFields);
    if (indexFields.empty()) {
        std::stringstream ss;
        ss << "index_fields can not be empty: package_name:" << GetIndexName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    for (auto indexFieldIt = indexFields.begin(); indexFieldIt != indexFields.end(); ++indexFieldIt) {
        auto indexField = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(*indexFieldIt);
        auto fieldElement = indexField.find(FieldConfig::FIELD_NAME);
        if (fieldElement == indexField.end()) {
            std::stringstream ss;
            ss << "no 'field_name' found in package " << GetIndexName();
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        std::string fieldName = autil::legacy::AnyCast<std::string>(fieldElement->second);
        fieldNames.push_back(fieldName);

        fieldElement = indexField.find(FIELD_BOOST);
        if (fieldElement == indexField.end()) {
            std::stringstream ss;
            ss << "no boost found for field: " << fieldName;
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
        boosts.push_back(autil::legacy::json::JsonNumberCast<int32_t>(fieldElement->second));
    }
    for (size_t i = 0; i < fieldNames.size(); ++i) {
        auto fieldConfig = resource.GetFieldConfig(fieldNames[i]);
        if (!fieldConfig) {
            INDEXLIB_FATAL_ERROR(Schema, "get field [%s] failed", fieldNames[i].c_str());
        }
        auto status = AddFieldConfig(fieldConfig, boosts[i]);
        THROW_IF_STATUS_ERROR(status);
    }
    if (GetAnalyzer().empty()) {
        auto status = SetDefaultAnalyzer();
        THROW_IF_STATUS_ERROR(status);
    }
}

} // namespace indexlibv2::config

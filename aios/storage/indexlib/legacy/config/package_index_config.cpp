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
#include "indexlib/config/package_index_config.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"
using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, PackageIndexConfig);

struct PackageIndexConfig::Impl {
    std::vector<int32_t> fieldBoosts;
    std::vector<firstocc_t> fieldHits;
    std::vector<int32_t> fieldIdxInPack; // map fieldId => fieldIdxInPack
    FieldConfigVector indexFields;
    int32_t totalFieldBoost = 0;
    FieldSchemaPtr fieldSchema;
    pos_t maxFirstOccInDoc = INVALID_POSITION;
    SectionAttributeConfigPtr sectionAttributeConfig;
    bool hasSectionAttribute = true;
};

PackageIndexConfig::PackageIndexConfig(const string& indexName, InvertedIndexType indexType)
    : IndexConfig(indexName, indexType)
    , mImpl(make_unique<Impl>())
{
    mImpl->sectionAttributeConfig.reset(new SectionAttributeConfig);
}

PackageIndexConfig::PackageIndexConfig(const PackageIndexConfig& other)
    : IndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
{
}

PackageIndexConfig::~PackageIndexConfig() {}

uint32_t PackageIndexConfig::GetFieldCount() const { return mImpl->indexFields.size(); }
IndexConfig::Iterator PackageIndexConfig::CreateIterator() const { return IndexConfig::Iterator(mImpl->indexFields); }

int32_t PackageIndexConfig::GetFieldBoost(fieldid_t id) const
{
    return (size_t)id < mImpl->fieldBoosts.size() ? mImpl->fieldBoosts[id] : 0;
}

void PackageIndexConfig::SetFieldBoost(fieldid_t id, int32_t boost)
{
    if (id + 1 > (fieldid_t)mImpl->fieldBoosts.size()) {
        mImpl->fieldBoosts.resize(id + 1, 0);
    }
    mImpl->totalFieldBoost += boost - mImpl->fieldBoosts[id];
    mImpl->fieldBoosts[id] = boost;
}

int32_t PackageIndexConfig::GetFieldIdxInPack(fieldid_t id) const
{
    return (size_t)id < mImpl->fieldIdxInPack.size() ? mImpl->fieldIdxInPack[id] : -1;
}

int32_t PackageIndexConfig::GetFieldIdxInPack(const std::string& fieldName) const
{
    fieldid_t fid = mImpl->fieldSchema->GetFieldId(fieldName);
    if (fid == INVALID_FIELDID) {
        return -1;
    }
    return GetFieldIdxInPack(fid);
}

fieldid_t PackageIndexConfig::GetFieldId(int32_t fieldIdxInPack) const
{
    if (fieldIdxInPack >= 0 && fieldIdxInPack < (int32_t)mImpl->indexFields.size()) {
        return mImpl->indexFields[fieldIdxInPack]->GetFieldId();
    }
    return INVALID_FIELDID;
}

int32_t PackageIndexConfig::GetTotalFieldBoost() const { return mImpl->totalFieldBoost; }

void PackageIndexConfig::AddFieldConfig(const FieldConfigPtr& fieldConfig, int32_t boost)
{
    assert(fieldConfig);
    assert(GetInvertedIndexType() == it_pack || GetInvertedIndexType() == it_expack ||
           GetInvertedIndexType() == it_customized);

    FieldType fieldType = fieldConfig->GetFieldType();
    if (!CheckFieldType(fieldType)) {
        stringstream ss;
        ss << "InvertedIndexType " << IndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType()) << " and FieldType "
           << FieldConfig::FieldTypeToStr(fieldType) << " Mismatch. index name [" << GetIndexName() << "]";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
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

    if (mImpl->indexFields.size() >= maxFieldNum) {
        stringstream ss;
        ss << "Only support at most " << maxFieldNum << " fields in package index, index name [" << GetIndexName()
           << "]";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    fieldid_t id = fieldConfig->GetFieldId();
    if (id < (fieldid_t)mImpl->fieldHits.size() && mImpl->fieldHits[id] != 0) {
        stringstream ss;
        ss << "duplicated field in package: " << GetIndexName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    if (fieldConfig->IsEnableNullField()) {
        for (auto& field : mImpl->indexFields) {
            if (field->IsEnableNullField() &&
                field->GetNullFieldLiteralString() != fieldConfig->GetNullFieldLiteralString()) {
                INDEXLIB_FATAL_ERROR(Schema,
                                     "enable null fields in package index [%s] "
                                     "with different null literal string.",
                                     GetIndexName().c_str());
            }
        }
    }
    mImpl->indexFields.push_back(fieldConfig);
    SetFieldBoost(id, boost);

    if (id + 1 > (fieldid_t)mImpl->fieldHits.size()) {
        mImpl->fieldHits.resize(id + 1, (firstocc_t)0);
    }
    mImpl->fieldHits[id] = mImpl->indexFields.size();

    if (id + 1 > (fieldid_t)mImpl->fieldIdxInPack.size()) {
        mImpl->fieldIdxInPack.resize(id + 1, (int32_t)-1);
    }
    mImpl->fieldIdxInPack[id] = mImpl->indexFields.size() - 1;
}
void PackageIndexConfig::AddFieldConfig(fieldid_t fieldId, int32_t boost)
{
    assert(mImpl->fieldSchema);
    FieldConfigPtr fieldConfig = mImpl->fieldSchema->GetFieldConfig(fieldId);
    if (!fieldConfig) {
        stringstream msg;
        msg << "No such field in field definition:" << fieldId << ", index name [" << GetIndexName() << "]";
        INDEXLIB_FATAL_ERROR(Schema, "%s", msg.str().c_str());
    }
    AddFieldConfig(fieldConfig, boost);
}
void PackageIndexConfig::AddFieldConfig(const std::string& fieldName, int32_t boost)
{
    assert(mImpl->fieldSchema);
    FieldConfigPtr fieldConfig = mImpl->fieldSchema->GetFieldConfig(fieldName);
    if (!fieldConfig) {
        stringstream ss;
        ss << "No such field in field definition: " << fieldName << ", index name [" << GetIndexName() << "]";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }
    AddFieldConfig(fieldConfig, boost);
}

bool PackageIndexConfig::HasSectionAttribute() const
{
    return mImpl->hasSectionAttribute && (GetOptionFlag() & of_position_list);
}

const SectionAttributeConfigPtr& PackageIndexConfig::GetSectionAttributeConfig() const
{
    return mImpl->sectionAttributeConfig;
}

void PackageIndexConfig::SetHasSectionAttributeFlag(bool flag) { mImpl->hasSectionAttribute = flag; }

const FieldConfigVector& PackageIndexConfig::GetFieldConfigVector() const { return mImpl->indexFields; }

std::vector<std::shared_ptr<indexlibv2::config::FieldConfig>> PackageIndexConfig::GetFieldConfigs() const
{
    return indexlib::config::FieldConfig::FieldConfigsToV2(mImpl->indexFields);
}

void PackageIndexConfig::SetFieldSchema(const FieldSchemaPtr& fieldSchema) { mImpl->fieldSchema = fieldSchema; }

bool PackageIndexConfig::IsInIndex(fieldid_t fieldId) const
{
    if (fieldId < 0 || fieldId >= (fieldid_t)mImpl->fieldIdxInPack.size()) {
        return false;
    }
    return mImpl->fieldIdxInPack[fieldId] >= 0;
}
void PackageIndexConfig::SetMaxFirstOccInDoc(pos_t pos) { mImpl->maxFirstOccInDoc = pos; }

pos_t PackageIndexConfig::GetMaxFirstOccInDoc() const { return mImpl->maxFirstOccInDoc; }

void PackageIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (IsVirtual()) {
        return;
    }

    if (json.GetMode() == TO_JSON) {
        IndexConfig::Jsonize(json);

        if (!mImpl->indexFields.empty()) {
            vector<JsonMap> indexFields;
            indexFields.reserve(mImpl->indexFields.size());
            for (FieldConfigVector::iterator it = mImpl->indexFields.begin(); it != mImpl->indexFields.end(); ++it) {
                JsonMap fieldBoostInfo;
                fieldBoostInfo[FIELD_NAME] = (*it)->GetFieldName();
                fieldBoostInfo[FIELD_BOOST] = GetFieldBoost((*it)->GetFieldId());
                indexFields.push_back(fieldBoostInfo);
            }
            json.Jsonize(INDEX_FIELDS, indexFields);
        }

        if (mImpl->hasSectionAttribute) {
            SectionAttributeConfig defaultSecAttrConfig;
            if (mImpl->sectionAttributeConfig && *mImpl->sectionAttributeConfig != defaultSecAttrConfig) {
                json.Jsonize(SECTION_ATTRIBUTE_CONFIG, mImpl->sectionAttributeConfig);
            }
        } else {
            json.Jsonize(HAS_SECTION_ATTRIBUTE, mImpl->hasSectionAttribute);
        }

        if (mImpl->maxFirstOccInDoc != INVALID_POSITION) {
            json.Jsonize(MAX_FIRSTOCC_IN_DOC, mImpl->maxFirstOccInDoc);
        }
    } else {
        IndexConfig::Jsonize(json);
        json.Jsonize(HAS_SECTION_ATTRIBUTE, mImpl->hasSectionAttribute, mImpl->hasSectionAttribute);
        if (mImpl->hasSectionAttribute) {
            json.Jsonize(SECTION_ATTRIBUTE_CONFIG, mImpl->sectionAttributeConfig, mImpl->sectionAttributeConfig);
        } else {
            mImpl->sectionAttributeConfig.reset();
        }
        json.Jsonize(MAX_FIRSTOCC_IN_DOC, mImpl->maxFirstOccInDoc, mImpl->maxFirstOccInDoc);
    }
}
void PackageIndexConfig::AssertEqual(const IndexConfig& other) const
{
    IndexConfig::AssertEqual(other);
    const PackageIndexConfig& other2 = (const PackageIndexConfig&)other;
    IE_CONFIG_ASSERT_EQUAL(mImpl->fieldBoosts, other2.mImpl->fieldBoosts, "fieldBoosts not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->totalFieldBoost, other2.mImpl->totalFieldBoost, "totalFieldBoost not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->indexFields.size(), other2.mImpl->indexFields.size(), "indexFields's size not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->hasSectionAttribute, other2.mImpl->hasSectionAttribute,
                           "hasSectionAttribute not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->maxFirstOccInDoc, other2.mImpl->maxFirstOccInDoc, "maxFirstOccInDoc not equal");
    for (size_t i = 0; i < mImpl->indexFields.size(); ++i) {
        mImpl->indexFields[i]->AssertEqual(*(other2.mImpl->indexFields[i]));
    }

    IE_CONFIG_ASSERT_EQUAL((GetDictConfig() != NULL), (other2.GetDictConfig() != NULL),
                           "high frequency dictionary config is not equal");
    if (GetDictConfig()) {
        auto status = GetDictConfig()->CheckEqual(*other2.GetDictConfig());
        THROW_IF_STATUS_ERROR(status);
    }
    IE_CONFIG_ASSERT_EQUAL(GetHighFrequencyTermPostingType(), other2.GetHighFrequencyTermPostingType(),
                           "high frequency term posting type is not equal");

    if (mImpl->sectionAttributeConfig && other2.mImpl->sectionAttributeConfig) {
        mImpl->sectionAttributeConfig->AssertEqual(*other2.mImpl->sectionAttributeConfig);
    } else if (mImpl->sectionAttributeConfig || other2.mImpl->sectionAttributeConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "section_attribute_config not equal!");
    }
}
void PackageIndexConfig::AssertCompatible(const IndexConfig& other) const { AssertEqual(other); }
IndexConfig* PackageIndexConfig::Clone() const { return new PackageIndexConfig(*this); }

std::unique_ptr<indexlibv2::config::InvertedIndexConfig> PackageIndexConfig::ConstructConfigV2() const
{
    return DoConstructConfigV2<indexlibv2::config::PackageIndexConfig>();
}

bool PackageIndexConfig::FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const
{
    if (!IndexConfig::FulfillConfigV2(configV2)) {
        AUTIL_LOG(ERROR, "fulfill package index config failed");
        return false;
    }
    auto packConfigV2 = dynamic_cast<indexlibv2::config::PackageIndexConfig*>(configV2);
    assert(packConfigV2);

    const auto& fieldConfigs = GetFieldConfigs();
    for (const auto& fieldConfig : fieldConfigs) {
        auto boost = GetFieldBoost(fieldConfig->GetFieldId());
        // status would always be ok
        [[maybe_unused]] auto status = packConfigV2->AddFieldConfig(fieldConfig, boost);
        assert(status.IsOK());
    }
    packConfigV2->SetMaxFirstOccInDoc(GetMaxFirstOccInDoc());
    packConfigV2->SetSectionAttributeConfig(GetSectionAttributeConfig());
    packConfigV2->SetHasSectionAttributeFlag(mImpl->hasSectionAttribute);
    return true;
}

void PackageIndexConfig::SetDefaultAnalyzer()
{
    string analyzer;
    for (size_t i = 0; i < mImpl->indexFields.size(); ++i) {
        if (analyzer.empty()) {
            analyzer = mImpl->indexFields[i]->GetAnalyzerName();
            continue;
        }

        if (analyzer != mImpl->indexFields[i]->GetAnalyzerName()) {
            stringstream ss;
            ss << "ambiguous analyzer name in pack index, index_name = " << GetIndexName()
               << ", analyzerName1 = " << analyzer << ", analyzerName2 = " << mImpl->indexFields[i]->GetAnalyzerName();
            INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
        }
    }
    SetAnalyzer(analyzer);
}

// only for test
void PackageIndexConfig::SetSectionAttributeConfig(SectionAttributeConfigPtr sectionAttributeConfig)
{
    mImpl->sectionAttributeConfig = sectionAttributeConfig;
}

void PackageIndexConfig::Check() const
{
    IndexConfig::Check();
    CheckDictHashMustBeDefault();
}

bool PackageIndexConfig::CheckFieldType(FieldType ft) const { return ft == ft_text; }

util::KeyValueMap PackageIndexConfig::GetDictHashParams() const { return {}; }

std::vector<std::string> PackageIndexConfig::GetIndexPath() const
{
    auto paths = IndexConfig::GetIndexPath();
    auto sectionAttributeConfig = GetSectionAttributeConfig();
    if (sectionAttributeConfig) {
        auto attributeConfig = sectionAttributeConfig->CreateAttributeConfig(GetIndexName());
        auto attrPaths = attributeConfig->GetIndexPath();
        paths.insert(paths.end(), attrPaths.begin(), attrPaths.end());
    }
    return paths;
}

void PackageIndexConfig::CheckDictHashMustBeDefault() const
{
    for (FieldConfigPtr fieldConifg : mImpl->indexFields) {
        auto hashFunc =
            util::GetTypeValueFromJsonMap<std::string>(fieldConifg->GetUserDefinedParam(), "dict_hash_func", "default");
        if (!hashFunc || *hashFunc != "default") {
            INDEXLIB_FATAL_ERROR(Schema,
                                 "index [%s] with type [%s], field [%s] not support undefault hash function [%s]",
                                 GetIndexName().c_str(), IndexConfig::InvertedIndexTypeToStr(GetInvertedIndexType()),
                                 fieldConifg->GetFieldName().c_str(), hashFunc->c_str());
        }
    }
}

void PackageIndexConfig::SetFileCompressConfig(const std::shared_ptr<FileCompressConfig>& compressConfig)
{
    IndexConfig::SetFileCompressConfig(compressConfig);
    if (mImpl->sectionAttributeConfig) {
        mImpl->sectionAttributeConfig->SetFileCompressConfig(compressConfig);
    }
}

}} // namespace indexlib::config

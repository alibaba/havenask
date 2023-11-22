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
#include "indexlib/document/normal/rewriter/AddToUpdateDocumentRewriter.h"

#include "autil/Scope.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/AttributeDocumentFieldExtractor.h"
#include "indexlib/document/normal/NormalDocument.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/config/TruncateIndexConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, AddToUpdateDocumentRewriter);

#define TABLET_LOG(level, format, args...)                                                                             \
    AUTIL_LOG(level, "[%s] [%p] " format, _schema->GetTableName().c_str(), this, ##args)

AddToUpdateDocumentRewriter::AddToUpdateDocumentRewriter() {}
AddToUpdateDocumentRewriter::~AddToUpdateDocumentRewriter() {}

Status AddToUpdateDocumentRewriter::Init(
    const std::shared_ptr<config::ITabletSchema>& schema,
    const std::vector<std::shared_ptr<config::TruncateOptionConfig>>& truncateOptionConfigs,
    const std::vector<config::SortDescriptions>& sortDescVec)

{
    assert(schema);
    _schema = schema;
    AllocFieldBitmap();
    AddUpdatableFields(sortDescVec);
    _attrFieldExtractor = std::make_unique<AttributeDocumentFieldExtractor>();
    auto status = _attrFieldExtractor->Init(_schema);
    RETURN_IF_STATUS_ERROR(status, "init attribute field extractor failed");

    for (size_t i = 0; i < truncateOptionConfigs.size(); i++) {
        if (truncateOptionConfigs[i]) {
            FilterTruncateSortFields(truncateOptionConfigs[i]);
        }
    }
    return Status::OK();
}

void AddToUpdateDocumentRewriter::AllocFieldBitmap()
{
    size_t fieldCount = _schema->GetFieldCount();

    _attributeUpdatableFieldIds.Clear();
    _attributeUpdatableFieldIds.Alloc(fieldCount);

    _invertedIndexUpdatableFieldIds.Clear();
    _invertedIndexUpdatableFieldIds.Alloc(fieldCount);

    _uselessFieldIds.Clear();
    _uselessFieldIds.Alloc(fieldCount);
}

void AddToUpdateDocumentRewriter::AddUpdatableFields(const std::vector<config::SortDescriptions>& sortDescVec)
{
    auto fieldConfigs = _schema->GetFieldConfigs();
    auto summaryConfigs = _schema->GetIndexConfigs(index::SUMMARY_INDEX_TYPE_STR);
    auto sourceConfig = std::dynamic_pointer_cast<config::SourceIndexConfig>(
        _schema->GetIndexConfig(index::SOURCE_INDEX_TYPE_STR, index::SOURCE_INDEX_NAME));
    std::shared_ptr<config::SummaryIndexConfig> summaryIndexConfig;

    if (!summaryConfigs.empty()) {
        assert(summaryConfigs.size() == 1u);
        summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(summaryConfigs[0]);
        assert(summaryIndexConfig);
    }
    for (const auto& fieldConfig : fieldConfigs) {
        fieldid_t fieldId = fieldConfig->GetFieldId();
        const std::string& fieldName = fieldConfig->GetFieldName();
        auto indexConfigs = _schema->GetIndexConfigsByFieldName(fieldName);
        if (indexConfigs.empty()) {
            SetUselessField(fieldId);
            continue;
        }
        if (IsSortField(fieldName, sortDescVec)) {
            continue;
        }
        if (sourceConfig) {
            auto sourceFieldConfigs = sourceConfig->GetFieldConfigs();
            bool inSource = false;
            for (const auto& fieldConfig : sourceFieldConfigs) {
                if (fieldConfig && fieldConfig->GetFieldName() == fieldName) {
                    inSource = true;
                    break;
                }
            }
            if (inSource) {
                AUTIL_LOG(INFO, "field [%s] is in source, not updatable", fieldName.c_str());
                continue;
            }
        }
        bool inSummary = summaryIndexConfig && summaryIndexConfig->IsInSummary(fieldId);

        bool inAttribute = false;
        bool inInvertedIndex = false;
        bool allIndexUpdatable = true;
        for (const auto& indexConfig : indexConfigs) {
            const auto& type = indexConfig->GetIndexType();
            if (type == index::ATTRIBUTE_INDEX_TYPE_STR) {
                auto [active, updatable] = CheckAttribute(indexConfig);
                if (active) {
                    inAttribute = true;
                    allIndexUpdatable &= updatable;
                }
            } else if (type == indexlib::index::INVERTED_INDEX_TYPE_STR) {
                auto [active, updatable] = CheckInvertedIndex(indexConfig);
                if (active) {
                    inInvertedIndex = true;
                    allIndexUpdatable &= updatable;
                }
            }
        }
        if (inSummary && !inAttribute) {
            continue;
        }
        if (!allIndexUpdatable) {
            continue;
        }
        SetField(fieldId, inAttribute, inInvertedIndex);
    }
}

void AddToUpdateDocumentRewriter::SetUselessField(fieldid_t fieldId)
{
    assert(fieldId >= 0);
    _uselessFieldIds.Set(fieldId);
}

void AddToUpdateDocumentRewriter::SetField(fieldid_t fieldId, bool inAttribute, bool inInvertedIndex)
{
    assert(fieldId >= 0);
    if (inAttribute) {
        _attributeUpdatableFieldIds.Set(fieldId);
    }
    if (inInvertedIndex) {
        _invertedIndexUpdatableFieldIds.Set(fieldId);
    }
}

// return {active, updatable}
std::pair<bool, bool>
AddToUpdateDocumentRewriter::CheckAttribute(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    const auto& attrConfig = std::dynamic_pointer_cast<index::AttributeConfig>(indexConfig);
    if (attrConfig) {
        if (attrConfig->IsDeleted()) {
            return {false, false};
        }
        return {true, attrConfig->IsAttributeUpdatable()};
    }
    const auto& packAttrConfig = std::dynamic_pointer_cast<index::PackAttributeConfig>(indexConfig);
    if (packAttrConfig) {
        return {true, packAttrConfig->IsPackAttributeUpdatable()};
    }
    assert(false);
    return {true, false};
}

// return {active, updatable}
std::pair<bool, bool>
AddToUpdateDocumentRewriter::CheckInvertedIndex(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    const auto& invertedIndexConfig = std::dynamic_pointer_cast<config::InvertedIndexConfig>(indexConfig);
    assert(invertedIndexConfig);
    return {true, invertedIndexConfig->IsIndexUpdatable()};
}

bool AddToUpdateDocumentRewriter::NeedRewrite(const std::shared_ptr<NormalDocument>& doc)
{
    if (doc->GetDocOperateType() != ADD_DOC) {
        return false;
    }
    if (!doc->GetAttributeDocument() &&
        (!doc->GetIndexDocument() || doc->GetIndexDocument()->GetModifiedTokens().empty())) {
        return false;
    }
    const std::vector<fieldid_t>& modifiedFields = doc->GetModifiedFields();
    if (modifiedFields.empty()) {
        return false;
    }
    // TODO: support sub doc
    return true;
}

void AddToUpdateDocumentRewriter::RewriteIndexDocument(const std::shared_ptr<NormalDocument>& doc)
{
    const auto& indexDoc = doc->GetIndexDocument();
    assert(indexDoc);
    std::string pkStr = indexDoc->GetPrimaryKey();
    auto modifiedTokens = indexDoc->StealModifiedTokens();
    indexDoc->ClearData();
    indexDoc->SetPrimaryKey(pkStr);
    indexDoc->SetModifiedTokens(std::move(modifiedTokens));
}

std::function<void()> AddToUpdateDocumentRewriter::TryRewrite(const std::shared_ptr<NormalDocument>& doc)
{
    autil::ScopeGuard guard([&doc]() mutable {
        doc->ClearModifiedFields();
        // TODO: sub doc?
    });

    if (!NeedRewrite(doc)) {
        return nullptr;
    }
    // TODO: summary store in attribute

    auto attrDoc = std::make_shared<indexlib::document::AttributeDocument>();
    const auto& oriAttrDoc = doc->GetAttributeDocument();
    assert(oriAttrDoc);
    const std::vector<fieldid_t>& modifiedFields = doc->GetModifiedFields();
    const auto& indexDoc = doc->GetIndexDocument();

    bool rewriteFailed = false;
    for (size_t i = 0; i < modifiedFields.size(); ++i) {
        fieldid_t fid = modifiedFields[i];
        if (_uselessFieldIds.Test(fid)) {
            continue;
        }
        bool indexUpdate = _invertedIndexUpdatableFieldIds.Test(fid);
        bool attrUpdate = _attributeUpdatableFieldIds.Test(fid);
        if (!indexUpdate && !attrUpdate) {
            TABLET_LOG(DEBUG, "doc (pk = %s) modified field [%s] is not updatable",
                       doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                       _schema->GetFieldConfig(fid)->GetFieldName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "doc (pk = %s) modified field [%s] is not updatable",
                                doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                                _schema->GetFieldConfig(fid)->GetFieldName().c_str());
            rewriteFailed = true;
            doc->AddModifyFailedField(fid);
            continue;
        }
        if (indexUpdate) {
            assert(indexDoc);
            if (indexDoc->GetFieldModifiedTokens(fid) == nullptr) {
                TABLET_LOG(DEBUG, "doc (pk = %s) modified field [%s] is not updatable(index update)",
                           doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                           _schema->GetFieldConfig(fid)->GetFieldName().c_str());
                ERROR_COLLECTOR_LOG(ERROR, "doc (pk = %s) modified field [%s] is not updatable(index update)",
                                    doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                                    _schema->GetFieldConfig(fid)->GetFieldName().c_str());
                rewriteFailed = true;
                doc->AddModifyFailedField(fid);
                continue;
            }
        }
        if (attrUpdate && !rewriteFailed) {
            attrDoc->SetField(fid, _attrFieldExtractor->GetField(oriAttrDoc, fid, doc->GetPool()));
        }
    }
    if (rewriteFailed) {
        return nullptr;
    }

    return [this, doc, attrDoc]() {
        RewriteIndexDocument(doc);
        doc->SetSummaryDocument(nullptr);
        doc->SetAttributeDocument(attrDoc);
        doc->ModifyDocOperateType(UPDATE_FIELD);
    };
}

Status AddToUpdateDocumentRewriter::RewriteOneDoc(const std::shared_ptr<NormalDocument>& doc)
{
    auto committer = TryRewrite(doc);
    if (committer) {
        committer();
    }
    return Status::OK();
}

void AddToUpdateDocumentRewriter::FilterTruncateSortFields(
    const std::shared_ptr<config::TruncateOptionConfig>& truncateOptionConfig)
{
    for (const auto& [_, truncateIndexConfig] : truncateOptionConfig->GetTruncateIndexConfigs()) {
        for (const auto& truncateIndexProperty : truncateIndexConfig.GetTruncateIndexProperties()) {
            FilterTruncateProfileField(truncateIndexProperty.truncateProfile);
            FilterTruncateStrategyField(truncateIndexProperty.truncateStrategy);
        }
    }
    return;
}

bool AddToUpdateDocumentRewriter::SetField(const std::string& fieldName)
{
    fieldid_t fid = _schema->GetFieldId(fieldName);
    if (fid == INVALID_FIELDID) {
        return false;
    }

    auto indexConfig = _schema->GetIndexConfig(indexlib::index::INVERTED_INDEX_TYPE_STR, fieldName);
    bool isInvertedField = indexConfig != nullptr ? true : false;

    indexConfig = _schema->GetIndexConfig(indexlibv2::index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
    bool isAttributeField = indexConfig != nullptr ? true : false;

    if (isInvertedField) {
        _invertedIndexUpdatableFieldIds.Reset(fid);
    }
    if (isAttributeField) {
        _attributeUpdatableFieldIds.Reset(fid);
    }

    return true;
}

void AddToUpdateDocumentRewriter::FilterTruncateProfileField(const std::shared_ptr<config::TruncateProfile>& profile)
{
    assert(profile != nullptr);
    for (const auto& sortParam : profile->sortParams) {
        const std::string& sortField = sortParam.GetSortField();
        if (!SetField(sortField)) {
            TABLET_LOG(WARN, "invalid field %s in %s", sortField.c_str(), profile->truncateProfileName.c_str());
            ERROR_COLLECTOR_LOG(WARN, "invalid field %s in %s", sortField.c_str(),
                                profile->truncateProfileName.c_str());
        }
    }
}

void AddToUpdateDocumentRewriter::FilterTruncateStrategyField(const std::shared_ptr<config::TruncateStrategy>& strategy)
{
    assert(strategy != nullptr);
    const std::string& filterField = strategy->GetDiversityConstrain().GetFilterField();
    if (filterField.empty()) {
        return;
    }
    if (!SetField(filterField)) {
        TABLET_LOG(WARN, "invalid field %s in %s", filterField.c_str(), strategy->GetStrategyName().c_str());
        ERROR_COLLECTOR_LOG(WARN, "invalid field %s in %s", filterField.c_str(), strategy->GetStrategyName().c_str());
    }
}

bool AddToUpdateDocumentRewriter::IsSortField(const std::string& fieldName,
                                              const std::vector<config::SortDescriptions>& sortDescVec)
{
    for (size_t i = 0; i < sortDescVec.size(); i++) {
        for (auto sortDesc : sortDescVec[i]) {
            if (sortDesc.GetSortFieldName() == fieldName) {
                return true;
            }
        }
    }
    return false;
}

} // namespace indexlibv2::document

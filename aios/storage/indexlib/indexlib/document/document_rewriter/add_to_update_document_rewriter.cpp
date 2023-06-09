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
#include "indexlib/document/document_rewriter/add_to_update_document_rewriter.h"

#include "autil/Scope.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, AddToUpdateDocumentRewriter);

AddToUpdateDocumentRewriter::AddToUpdateDocumentRewriter() {}

AddToUpdateDocumentRewriter::~AddToUpdateDocumentRewriter() {}

void AddToUpdateDocumentRewriter::Init(const IndexPartitionSchemaPtr& schema,
                                       const vector<TruncateOptionConfigPtr>& truncateOptionConfigs,
                                       const vector<indexlibv2::config::SortDescriptions>& sortDescVec)
{
    mIndexSchema = schema->GetIndexSchema();
    mAttrSchema = schema->GetAttributeSchema();
    mFieldSchema = schema->GetFieldSchema();
    AllocFieldBitmap();
    AddUpdatableFields(schema, sortDescVec);
    mAttrFieldExtractor.reset(new AttributeDocumentFieldExtractor());

    if (!mAttrFieldExtractor->Init(schema->GetAttributeSchema())) {
        INDEXLIB_FATAL_ERROR(InitializeFailed, "fail to init AttributeDocumentFieldExtractor!");
    }

    for (size_t i = 0; i < truncateOptionConfigs.size(); i++) {
        FilterTruncateSortFields(truncateOptionConfigs[i]);
    }
}

void AddToUpdateDocumentRewriter::AllocFieldBitmap()
{
    assert(mFieldSchema);
    size_t fieldCount = mFieldSchema->GetFieldCount();

    mAttrUpdatableFieldIds.Clear();
    mAttrUpdatableFieldIds.Alloc(fieldCount);

    mIndexUpdatableFieldIds.Clear();
    mIndexUpdatableFieldIds.Alloc(fieldCount);

    mUselessFieldIds.Clear();
    mUselessFieldIds.Alloc(fieldCount);
}

void AddToUpdateDocumentRewriter::AddUpdatableFields(const IndexPartitionSchemaPtr& schema,
                                                     const vector<indexlibv2::config::SortDescriptions>& sortDescVec)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
    assert(indexSchema);
    assert(attrSchema);

    FieldSchema::Iterator iter = mFieldSchema->Begin();
    for (; iter != mFieldSchema->End(); iter++) {
        const FieldConfigPtr& fieldConfig = *iter;
        if (fieldConfig->IsDeleted()) {
            continue;
        }

        fieldid_t fieldId = fieldConfig->GetFieldId();
        const string& fieldName = fieldConfig->GetFieldName();

        if (!schema->IsUsefulField(fieldName)) {
            SetUselessField(fieldName);
            continue;
        }
        if (summarySchema && summarySchema->IsInSummary(fieldId) && !attrSchema->IsInAttribute(fieldId)) {
            continue;
        }

        // index, attr
        if (!indexSchema->AllIndexUpdatableForField(fieldId)) {
            continue;
        }
        if (attrSchema->IsInAttribute(fieldId)) {
            auto attrConfig = attrSchema->GetAttributeConfigByFieldId(fieldId);
            assert(attrConfig);
            if (!attrConfig->IsAttributeUpdatable() || IsSortField(fieldName, sortDescVec)) {
                continue;
            }
        }
        SetField(fieldName, true);
    }
}

bool AddToUpdateDocumentRewriter::IsSortField(const string& fieldName,
                                              const vector<indexlibv2::config::SortDescriptions>& sortDescVec)
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

void AddToUpdateDocumentRewriter::FilterTruncateSortFields(const TruncateOptionConfigPtr& truncOptionConfig)
{
    if (!truncOptionConfig) {
        return;
    }

    const TruncateIndexConfigMap& truncateIndexConfigs = truncOptionConfig->GetTruncateIndexConfigs();
    TruncateIndexConfigMap::const_iterator iter = truncateIndexConfigs.begin();
    for (; iter != truncateIndexConfigs.end(); ++iter) {
        const TruncateIndexPropertyVector& truncIndexPropeties = iter->second.GetTruncateIndexProperties();
        for (size_t i = 0; i < truncIndexPropeties.size(); ++i) {
            const TruncateProfilePtr& profile = truncIndexPropeties[i].mTruncateProfile;
            FilterTruncateProfileField(profile);

            const TruncateStrategyPtr& strategy = truncIndexPropeties[i].mTruncateStrategy;
            FilterTruncateStrategyField(strategy);
        }
    }
}

void AddToUpdateDocumentRewriter::FilterTruncateProfileField(const TruncateProfilePtr& profile)
{
    assert(profile);
    const SortParams& sortParams = profile->mSortParams;
    for (size_t j = 0; j < sortParams.size(); ++j) {
        const string& sortField = sortParams[j].GetSortField();
        if (!SetField(sortField, false)) {
            IE_LOG(WARN, "invalid field %s in %s", sortField.c_str(), profile->mTruncateProfileName.c_str());
            ERROR_COLLECTOR_LOG(WARN, "invalid field %s in %s", sortField.c_str(),
                                profile->mTruncateProfileName.c_str());
        }
    }
}

bool AddToUpdateDocumentRewriter::SetUselessField(const string& uselessFieldName)
{
    fieldid_t fid = mFieldSchema->GetFieldId(uselessFieldName);
    if (fid == INVALID_FIELDID) {
        return false;
    }

    mUselessFieldIds.Set(fid);
    return true;
}

bool AddToUpdateDocumentRewriter::SetField(const string& fieldName, bool isUpdatable)
{
    fieldid_t fid = mFieldSchema->GetFieldId(fieldName);
    if (fid == INVALID_FIELDID) {
        return false;
    }
    bool isIndexField = mIndexSchema->IsInIndex(fid);
    bool isAttrField = mAttrSchema->IsInAttribute(fid);
    if (isUpdatable) {
        if (isIndexField) {
            mIndexUpdatableFieldIds.Set(fid);
        }
        if (isAttrField) {
            mAttrUpdatableFieldIds.Set(fid);
        }
    } else {
        if (isIndexField) {
            mIndexUpdatableFieldIds.Reset(fid);
        }
        if (isAttrField) {
            mAttrUpdatableFieldIds.Reset(fid);
        }
    }
    return true;
}

void AddToUpdateDocumentRewriter::FilterTruncateStrategyField(const TruncateStrategyPtr& strategy)
{
    assert(strategy);
    const string& filterField = strategy->GetDiversityConstrain().GetFilterField();
    if (filterField.empty()) {
        return;
    }
    if (!SetField(filterField, false)) {
        IE_LOG(WARN, "invalid field %s in %s", filterField.c_str(), strategy->GetStrategyName().c_str());
        ERROR_COLLECTOR_LOG(WARN, "invalid field %s in %s", filterField.c_str(), strategy->GetStrategyName().c_str());
    }
}

void AddToUpdateDocumentRewriter::RewriteIndexDocument(const document::NormalDocumentPtr& doc)
{
    const IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    if (!indexDoc) {
        INDEXLIB_FATAL_ERROR(UnSupported, "indexDocument is NULL!");
    }

    string pkStr = indexDoc->GetPrimaryKey();
    std::vector<ModifiedTokens> modifiedTokens = indexDoc->StealModifiedTokens();
    indexDoc->ClearData();
    indexDoc->SetPrimaryKey(pkStr);
    indexDoc->SetModifiedTokens(std::move(modifiedTokens));
}

void AddToUpdateDocumentRewriter::Rewrite(const DocumentPtr& document)
{
    auto committer = TryRewrite(DYNAMIC_POINTER_CAST(NormalDocument, document));
    if (committer) {
        committer();
    }
}

std::function<void()> AddToUpdateDocumentRewriter::TryRewrite(const NormalDocumentPtr& doc)
{
    autil::ScopeGuard guard([&doc]() mutable {
        doc->ClearModifiedFields();
        doc->ClearSubModifiedFields();
    });

    if (!NeedRewrite(doc)) {
        return nullptr;
    }
    // TODO: summary store in attribute

    AttributeDocumentPtr attrDoc(new AttributeDocument);
    const AttributeDocumentPtr& oriAttrDoc = doc->GetAttributeDocument();
    assert(oriAttrDoc);
    const std::vector<fieldid_t>& modifiedFields = doc->GetModifiedFields();
    const auto& indexDoc = doc->GetIndexDocument();

    bool rewriteFailed = false;
    for (size_t i = 0; i < modifiedFields.size(); ++i) {
        fieldid_t fid = modifiedFields[i];
        if (mUselessFieldIds.Test(fid)) {
            continue;
        }
        bool indexUpdate = mIndexUpdatableFieldIds.Test(fid);
        bool attrUpdate = mAttrUpdatableFieldIds.Test(fid);
        if (!indexUpdate and !attrUpdate) {
            IE_LOG(DEBUG, "doc (pk = %s) modified field [%s] is not updatable",
                   doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                   mFieldSchema->GetFieldConfig(fid)->GetFieldName().c_str());
            ERROR_COLLECTOR_LOG(ERROR, "doc (pk = %s) modified field [%s] is not updatable",
                                doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                                mFieldSchema->GetFieldConfig(fid)->GetFieldName().c_str());
            rewriteFailed = true;
            doc->AddModifyFailedField(fid);
            continue;
        }
        if (indexUpdate) {
            assert(indexDoc);
            if (indexDoc->GetFieldModifiedTokens(fid) == nullptr) {
                IE_LOG(DEBUG, "doc (pk = %s) modified field [%s] is not updatable(index update)",
                       doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                       mFieldSchema->GetFieldConfig(fid)->GetFieldName().c_str());
                ERROR_COLLECTOR_LOG(ERROR, "doc (pk = %s) modified field [%s] is not updatable(index update)",
                                    doc->GetIndexDocument()->GetPrimaryKey().c_str(),
                                    mFieldSchema->GetFieldConfig(fid)->GetFieldName().c_str());
                rewriteFailed = true;
                doc->AddModifyFailedField(fid);
                continue;
            }
        }
        if (attrUpdate and !rewriteFailed) {
            attrDoc->SetField(fid, mAttrFieldExtractor->GetField(oriAttrDoc, fid, doc->GetPool()));
        }
    }
    if (rewriteFailed) {
        return nullptr;
    }

    return [this, doc, attrDoc]() {
        RewriteIndexDocument(doc);
        doc->SetSummaryDocument(SerializedSummaryDocumentPtr());
        doc->SetAttributeDocument(attrDoc);
        doc->ModifyDocOperateType(UPDATE_FIELD);
    };
}

bool AddToUpdateDocumentRewriter::NeedRewrite(const NormalDocumentPtr& doc)
{
    if (doc->GetDocOperateType() != ADD_DOC) {
        return false;
    }
    if (!doc->GetAttributeDocument() and
        (!doc->GetIndexDocument() or doc->GetIndexDocument()->GetModifiedTokens().empty())) {
        return false;
    }
    const std::vector<fieldid_t>& modifiedFields = doc->GetModifiedFields();
    if (modifiedFields.empty()) {
        return false;
    }
    // TODO: support sub doc
    if (!doc->GetSubModifiedFields().empty()) {
        return false;
    }
    return true;
}
}} // namespace indexlib::document

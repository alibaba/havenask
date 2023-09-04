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
#include "indexlib/index/attribute/UpdateFieldExtractor.h"

#include "indexlib/document/normal/AttributeDocument.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, UpdateFieldExtractor);

UpdateFieldExtractor::UpdateFieldExtractor() {}

void UpdateFieldExtractor::Init(const std::shared_ptr<config::IIndexConfig>& primaryKeyIndexConfig,
                                const std::vector<std::shared_ptr<config::IIndexConfig>>& attrConfigs,
                                const std::vector<std::shared_ptr<config::FieldConfig>>& fields)
{
    _primaryKeyIndexConfig = std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(primaryKeyIndexConfig);
    for (const auto& indexConfig : attrConfigs) {
        auto attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
        if (attrConfig != nullptr) {
            _attrConfigs.emplace_back(attrConfig);
        }
    }
    _fields = fields;
}

bool UpdateFieldExtractor::LoadFieldsFromDoc(indexlib::document::AttributeDocument* attrDoc)
{
    assert(_fieldVector.empty());
    if (!attrDoc) {
        AUTIL_LOG(WARN, "attribute document is NULL!");
        ERROR_COLLECTOR_LOG(WARN, "attribute document is NULL!");
        return false;
    }

    if (_attrConfigs.size() <= 0) {
        AUTIL_LOG(WARN, "no attribute schema found!");
        ERROR_COLLECTOR_LOG(WARN, "no attribute schema found!");
        return false;
    }
    fieldid_t fieldId = INVALID_FIELDID;
    AttributeDocument::Iterator it = attrDoc->CreateIterator();
    while (it.HasNext()) {
        const StringView& fieldValue = it.Next(fieldId);
        bool needIgnore = false;
        if (!CheckFieldId(fieldId, fieldValue, false, &needIgnore)) {
            _fieldVector.clear();
            return false;
        }
        if (!needIgnore) {
            _fieldVector.push_back(make_pair(fieldId, fieldValue));
        }
    }

    vector<fieldid_t> nullFieldIds;
    attrDoc->GetNullFieldIds(nullFieldIds);
    for (auto fieldId : nullFieldIds) {
        bool needIgnore = false;
        if (!CheckFieldId(fieldId, StringView::empty_instance(), true, &needIgnore)) {
            _fieldVector.clear();
            return false;
        }
        if (!needIgnore) {
            _fieldVector.push_back(make_pair(fieldId, StringView::empty_instance()));
        }
    }
    return true;
}

void UpdateFieldExtractor::Reset() { _fieldVector.clear(); }

bool UpdateFieldExtractor::GetFieldValue(const std::shared_ptr<indexlib::document::AttributeDocument>& attrDoc,
                                         fieldid_t fieldId, bool* isNull, autil::StringView* fieldValue)
{
    assert(attrDoc);
    *fieldValue = attrDoc->GetField(fieldId, *isNull);
    bool needIgnore = false;
    if (!CheckFieldId(fieldId, *fieldValue, *isNull, &needIgnore)) {
        return false;
    }
    if (needIgnore) {
        return false;
    }
    return true;
}

bool UpdateFieldExtractor::CheckFieldId(fieldid_t fieldId, const autil::StringView& fieldValue, bool isNull,
                                        bool* needIgnore)
{
    // todo: add check for delete field
    // if (!fieldConfig) {
    //     if (legacySchema->HasModifyOperations() && fieldId < (fieldid_t)fieldSchema->GetFieldCount()) {
    //         AUTIL_LOG(INFO, "field id [%d] is deleted, will ignore!", fieldId);
    //         needIgnore = true;
    //         return true;
    //     }
    //     AUTIL_LOG(ERROR,
    //               "fieldId [%d] in update document not in schema, "
    //               "drop the doc",
    //               fieldId);
    //     ERROR_COLLECTOR_LOG(ERROR,
    //                         "fieldId [%d] in update document not in schema, "
    //                         "drop the doc",
    //                         fieldId);
    //     return false;
    // }

    for (const auto& field : _fields) {
        if (field->GetFieldId() == fieldId) {
            *needIgnore = IsFieldIgnore(field, fieldValue, isNull);
            return true;
        }
    }

    return false;
}

} // namespace indexlibv2::index

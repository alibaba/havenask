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
#include "indexlib/index/normal/attribute/update_field_extractor.h"

#include "indexlib/document/index_document/normal_document/attribute_document.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, UpdateFieldExtractor);

bool UpdateFieldExtractor::Init(const document::AttributeDocumentPtr& attrDoc)
{
    assert(mFieldVector.empty());
    if (!attrDoc) {
        IE_LOG(WARN, "attribute document is NULL!");
        ERROR_COLLECTOR_LOG(WARN, "attribute document is NULL!");
        return false;
    }

    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema) {
        IE_LOG(WARN, "no attribute schema found!");
        ERROR_COLLECTOR_LOG(WARN, "no attribute schema found!");
        return false;
    }
    fieldid_t fieldId = INVALID_FIELDID;
    AttributeDocument::Iterator it = attrDoc->CreateIterator();
    while (it.HasNext()) {
        const StringView& fieldValue = it.Next(fieldId);
        bool needIgnore = false;
        if (!CheckFieldId(mSchema, fieldId, fieldValue, false, needIgnore)) {
            mFieldVector.clear();
            return false;
        }
        if (!needIgnore) {
            mFieldVector.push_back(make_pair(fieldId, fieldValue));
        }
    }

    vector<fieldid_t> nullFieldIds;
    attrDoc->GetNullFieldIds(nullFieldIds);
    for (auto fieldId : nullFieldIds) {
        bool needIgnore = false;
        if (!CheckFieldId(mSchema, fieldId, StringView::empty_instance(), true, needIgnore)) {
            mFieldVector.clear();
            return false;
        }
        if (!needIgnore) {
            mFieldVector.push_back(make_pair(fieldId, StringView::empty_instance()));
        }
    }
    return true;
}

bool UpdateFieldExtractor::GetFieldValue(const config::IndexPartitionSchemaPtr& schema,
                                         const document::AttributeDocumentPtr& attrDoc, fieldid_t fieldId, bool* isNull,
                                         autil::StringView* fieldValue)
{
    assert(attrDoc);
    assert(schema && schema->GetAttributeSchema());

    *fieldValue = attrDoc->GetField(fieldId, *isNull); // TODO: @qingran optimize
    bool needIgnore = false;
    if (!CheckFieldId(schema, fieldId, *fieldValue, *isNull, needIgnore)) {
        return false;
    }
    if (needIgnore) {
        return false;
    }
    return true;
}

bool UpdateFieldExtractor::CheckFieldId(const config::IndexPartitionSchemaPtr& schema, fieldid_t fieldId,
                                        const autil::StringView& fieldValue, bool isNull, bool& needIgnore)
{
    const FieldConfigPtr& fieldConfig = schema->GetFieldConfig(fieldId);
    if (!fieldConfig) {
        if (schema->HasModifyOperations() && fieldId < (fieldid_t)schema->GetFieldCount()) {
            IE_LOG(INFO, "field id [%d] is deleted, will ignore!", fieldId);
            needIgnore = true;
            return true;
        }
        ERROR_COLLECTOR_LOG(ERROR,
                            "fieldId [%d] in update document not in schema, "
                            "drop the field",
                            fieldId);
        return false;
    }
    needIgnore = IsFieldIgnore(schema, fieldConfig, fieldValue, isNull);
    return true;
}

}} // namespace indexlib::index

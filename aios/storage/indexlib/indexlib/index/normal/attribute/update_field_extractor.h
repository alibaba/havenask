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
#pragma once

#include <memory>

#include "indexlib/common/field_format/attribute/var_num_attribute_formatter.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/DefragSliceArray.h"

DECLARE_REFERENCE_CLASS(document, AttributeDocument);

namespace indexlib { namespace index {

class UpdateFieldExtractor
{
private:
    typedef std::pair<fieldid_t, autil::StringView> FieldItem;
    typedef std::vector<FieldItem> FieldVector;

public:
    class Iterator
    {
    public:
        Iterator(const FieldVector& fieldVec) : mFields(fieldVec) { mCursor = mFields.begin(); }

    public:
        bool HasNext() const { return mCursor != mFields.end(); }

        const autil::StringView& Next(fieldid_t& fieldId, bool& isNull)
        {
            assert(mCursor != mFields.end());
            fieldId = mCursor->first;
            const autil::StringView& value = mCursor->second;
            mCursor++;
            isNull = (value == autil::StringView::empty_instance());
            return value;
        }

    private:
        const FieldVector& mFields;
        FieldVector::const_iterator mCursor;
    };

public:
    static bool GetFieldValue(const config::IndexPartitionSchemaPtr& schema,
                              const document::AttributeDocumentPtr& attrDoc, fieldid_t fieldId, bool* isNull,
                              autil::StringView* fieldValue);

public:
    UpdateFieldExtractor(const config::IndexPartitionSchemaPtr& schema) : mSchema(schema)
    {
        const config::IndexSchemaPtr& indexSchema = mSchema->GetIndexSchema();
        assert(indexSchema);
        mPrimaryKeyFieldId = indexSchema->GetPrimaryKeyIndexFieldId();
    }

    ~UpdateFieldExtractor() {}

public:
    bool Init(const document::AttributeDocumentPtr& attrDoc);
    size_t GetFieldCount() const { return mFieldVector.size(); }
    Iterator CreateIterator() const { return Iterator(mFieldVector); }

private:
    static bool CheckFieldId(const config::IndexPartitionSchemaPtr& schema, fieldid_t fieldId,
                             const autil::StringView& fieldValue, bool isNull, bool& needIgnore);

    static bool IsFieldIgnore(const config::IndexPartitionSchemaPtr& schema, const config::FieldConfigPtr& fieldConfig,
                              const autil::StringView& fieldValue, bool isNull);

private:
    config::IndexPartitionSchemaPtr mSchema;
    fieldid_t mPrimaryKeyFieldId;
    FieldVector mFieldVector;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UpdateFieldExtractor);
/////////////////////////////////////////////////////////////
inline bool UpdateFieldExtractor::IsFieldIgnore(const config::IndexPartitionSchemaPtr& schema,
                                                const config::FieldConfigPtr& fieldConfig,
                                                const autil::StringView& fieldValue, bool isNull)
{
    if (!isNull && fieldValue.empty()) {
        return true;
    }

    if (isNull && !fieldConfig->IsEnableNullField()) {
        return true;
    }

    const std::string& fieldName = fieldConfig->GetFieldName();
    fieldid_t fieldId = fieldConfig->GetFieldId();
    if (fieldId == schema->GetIndexSchema()->GetPrimaryKeyIndexFieldId()) {
        IE_LOG(DEBUG, "field[%s] is primarykey field, ignore", fieldName.c_str());
        return true;
    }

    auto attrConfig = schema->GetAttributeSchema()->GetAttributeConfigByFieldId(fieldId);
    if (!attrConfig) {
        IE_LOG(DEBUG, "no attribute for field[%s], ignore", fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "no attribute for field[%s], ignore", fieldName.c_str());
        return true;
    }
    if (!attrConfig->IsAttributeUpdatable()) {
        // disabled attribute triggers this log, do not use level above DEBUG
        IE_LOG(DEBUG, "Unsupported updatable field[%s], ignore", fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "Unsupported updatable field[%s], ignore", fieldName.c_str());
        return true;
    }

    const config::AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    assert(attrSchema);
    if (!attrSchema->IsInAttribute(fieldId)) {
        IE_LOG(DEBUG, "no attribute for field[%s], ignore", fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "no attribute for field[%s], ignore", fieldName.c_str());
        return true;
    }

    // TODO: remove when defrag slice array support length over 64MB
    if (util::DefragSliceArray::IsOverLength(fieldValue.size(),
                                             common::VarNumAttributeFormatter::VAR_NUM_ATTRIBUTE_SLICE_LEN)) {
        IE_LOG(DEBUG, "attribute for field[%s] is overlength, ignore", fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "attribute for field[%s] is overlength, ignore", fieldName.c_str());
        return true;
    }
    return false;
}
}} // namespace indexlib::index

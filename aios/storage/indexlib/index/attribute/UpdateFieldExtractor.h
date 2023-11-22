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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/slice_array/DefragSliceArray.h"

namespace indexlib::document {
class AttributeDocument;
}

namespace indexlibv2::index {

class UpdateFieldExtractor : private autil::NoCopyable
{
public:
    UpdateFieldExtractor();
    ~UpdateFieldExtractor() {}

private:
    typedef std::pair<fieldid_t, autil::StringView> FieldItem;
    typedef std::vector<FieldItem> FieldVector;

public:
    class Iterator
    {
    public:
        Iterator(const FieldVector& fieldVec) : _fields(fieldVec) { _cursor = _fields.begin(); }

    public:
        bool HasNext() const { return _cursor != _fields.end(); }

        const autil::StringView& Next(fieldid_t& fieldId, bool& isNull)
        {
            assert(_cursor != _fields.end());
            fieldId = _cursor->first;
            const autil::StringView& value = _cursor->second;
            _cursor++;
            isNull = (value == autil::StringView::empty_instance());
            return value;
        }

    private:
        const FieldVector& _fields;
        FieldVector::const_iterator _cursor;
    };

public:
    void Init(const std::shared_ptr<config::IIndexConfig>& primaryKeyIndexConfig,
              const std::vector<std::shared_ptr<config::IIndexConfig>>& attrConfigs,
              const std::vector<std::shared_ptr<config::FieldConfig>>& fields);
    void Reset();

public:
    bool GetFieldValue(const std::shared_ptr<indexlib::document::AttributeDocument>& attrDoc, fieldid_t fieldId,
                       bool* isNull, autil::StringView* fieldValue);

public:
    bool LoadFieldsFromDoc(indexlib::document::AttributeDocument* attrDoc);
    size_t GetFieldCount() const { return _fieldVector.size(); }
    Iterator CreateIterator() const { return Iterator(_fieldVector); }

private:
    bool CheckFieldId(fieldid_t fieldId, const autil::StringView& fieldValue, bool isNull, bool* needIgnore);

    bool IsFieldIgnore(const std::shared_ptr<config::FieldConfig>& field, const autil::StringView& fieldValue,
                       bool isNull);

private:
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> _primaryKeyIndexConfig;
    std::vector<std::shared_ptr<AttributeConfig>> _attrConfigs;
    std::vector<std::shared_ptr<config::FieldConfig>> _fields;

    FieldVector _fieldVector;

private:
    AUTIL_LOG_DECLARE();
};

inline bool UpdateFieldExtractor::IsFieldIgnore(const std::shared_ptr<config::FieldConfig>& fieldConfig,
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
    if (_primaryKeyIndexConfig != nullptr) {
        auto pkFieldConfig = _primaryKeyIndexConfig->GetFieldConfig();
        if (fieldId == pkFieldConfig->GetFieldId()) {
            AUTIL_LOG(DEBUG, "field[%s] is primarykey field, ignore", fieldName.c_str());
            return true;
        }
    }

    bool isAttrField = false;
    for (const auto& attrConfig : _attrConfigs) {
        if (attrConfig->GetFieldId() == fieldId) {
            if (!attrConfig->IsAttributeUpdatable()) {
                AUTIL_LOG(WARN, "Unsupported updatable field[%s], ignore", fieldName.c_str());
                ERROR_COLLECTOR_LOG(WARN, "Unsupported updatable field[%s], ignore", fieldName.c_str());
                return true;
            }
            isAttrField = true;
            break;
        }
    }
    if (!isAttrField) {
        AUTIL_LOG(DEBUG, "no attribute for field[%s], ignore", fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "no attribute for field[%s], ignore", fieldName.c_str());
        return true;
    }

    // TODO: remove when defrag slice array support length over 64MB
    if (indexlib::util::DefragSliceArray::IsOverLength(fieldValue.size(), index::ATTRIBUTE_DEFAULT_SLICE_LEN)) {
        AUTIL_LOG(DEBUG, "attribute for field[%s] is overlength, ignore", fieldName.c_str());
        ERROR_COLLECTOR_LOG(WARN, "attribute for field[%s] is overlength, ignore", fieldName.c_str());
        return true;
    }
    return false;
}

} // namespace indexlibv2::index

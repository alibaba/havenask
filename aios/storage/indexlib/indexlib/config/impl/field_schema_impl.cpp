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
#include "indexlib/config/impl/field_schema_impl.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, FieldSchemaImpl);

FieldSchemaImpl::FieldSchemaImpl(size_t capacity) : mBaseFieldCount(-1), mOnlyAddVirtual(false)
{
    mFields.reserve(capacity);
}

void FieldSchemaImpl::AddFieldConfig(const FieldConfigPtr& fieldConfig)
{
    assert(fieldConfig);
    AddFieldConfig(fieldConfig, mNameToIdMap, mFields);
}

void FieldSchemaImpl::AddFieldConfig(const FieldConfigPtr& fieldConfig, NameMap& nameMap,
                                     vector<FieldConfigPtr>& fields)
{
    if (mOnlyAddVirtual && !fieldConfig->IsVirtual()) {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "add field [%s] fail, "
                             "only support add virtual field!",
                             fieldConfig->GetFieldName().c_str());
    }

    fieldid_t fieldId = mFields.size();
    NameMap::iterator it = nameMap.find(fieldConfig->GetFieldName());
    if (it != nameMap.end()) {
        stringstream ss;
        ss << "Duplicated field name:" << fieldConfig->GetFieldName();
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    fieldConfig->SetFieldId(fieldId);
    fields.push_back(fieldConfig);
    nameMap.insert(make_pair(fieldConfig->GetFieldName(), fieldConfig->GetFieldId()));
}

FieldConfigPtr FieldSchemaImpl::GetFieldConfig(fieldid_t id) const
{
    if (id < 0 || id >= (fieldid_t)mFields.size()) {
        return FieldConfigPtr();
    }

    FieldConfigPtr ret = mFields[id];
    if (ret && !ret->IsDeleted()) {
        return ret;
    }
    return FieldConfigPtr();
}

bool FieldSchemaImpl::IsDeleted(fieldid_t fieldId) const
{
    if (fieldId < 0 || fieldId >= (fieldid_t)mFields.size()) {
        return false;
    }
    return mFields[fieldId]->IsDeleted();
}

FieldConfigPtr FieldSchemaImpl::GetFieldConfig(const string& fieldName) const
{
    fieldid_t fieldId = GetFieldId(fieldName);
    if (fieldId == INVALID_FIELDID) {
        return FieldConfigPtr();
    }
    return GetFieldConfig(fieldId);
}

fieldid_t FieldSchemaImpl::GetFieldId(const std::string& fieldName) const
{
    NameMap::const_iterator it = mNameToIdMap.find(fieldName);
    if (it == mNameToIdMap.end()) {
        return INVALID_FIELDID;
    }
    return it->second;
}

bool FieldSchemaImpl::IsFieldNameInSchema(const std::string& fieldName) const
{
    fieldid_t id = GetFieldId(fieldName);
    return id != INVALID_FIELDID;
}

bool FieldSchemaImpl::IsFieldTypeSortable(const std::string& fieldName, const std::string& fieldType) const
{
    FieldConfigPtr fieldConfig = GetFieldConfig(fieldName);
    if (!fieldConfig) {
        return false;
    }

    bool isMultiValue = fieldConfig->IsMultiValue();
    if (isMultiValue) {
        return false;
    }

    FieldType expectType = fieldConfig->GetFieldType();
    FieldType type = FieldConfig::StrToFieldType(fieldType);
    if (type != expectType) {
        return false;
    }

    for (uint32_t i = 0; i < FIELD_SORTABLE_COUNT; ++i) {
        if (FIELD_SORTABLE_TYPE[i] == type) {
            return true;
        }
    }
    return false;
}

void FieldSchemaImpl::AssertEqual(const FieldSchemaImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mNameToIdMap, other.mNameToIdMap, "mNameToIdMap not equal");
    IE_CONFIG_ASSERT_EQUAL(mFields.size(), other.mFields.size(), "mFields' size not equal");
    size_t i;
    for (i = 0; i < mFields.size(); ++i) {
        mFields[i]->AssertEqual(*(other.mFields[i]));
    }
}

void FieldSchemaImpl::AssertCompatible(const FieldSchemaImpl& other) const
{
    IE_CONFIG_ASSERT(mNameToIdMap.size() <= other.mNameToIdMap.size(), "mNameToIdMap's size not compatible");
    IE_CONFIG_ASSERT(mFields.size() <= other.mFields.size(), "mFields' size not compatible");

    size_t i;
    for (i = 0; i < mFields.size(); ++i) {
        mFields[i]->AssertEqual(*(other.mFields[i]));
    }
}

void FieldSchemaImpl::DeleteField(const std::string& field)
{
    FieldConfigPtr fieldConfig = GetFieldConfig(field);
    if (!fieldConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "delete field [%s] fail, not exist!", field.c_str());
    }
    fieldConfig->Delete();
    mNameToIdMap.erase(field);
}

void FieldSchemaImpl::SetBaseSchemaImmutable()
{
    if (mBaseFieldCount < 0) {
        mBaseFieldCount = (int32_t)mFields.size();
    }
}

void FieldSchemaImpl::SetModifySchemaImmutable()
{
    if (mBaseFieldCount < 0) {
        mBaseFieldCount = (int32_t)mFields.size();
    }
    mOnlyAddVirtual = true;
}

void FieldSchemaImpl::SetModifySchemaMutable() { mOnlyAddVirtual = false; }

void FieldSchemaImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        // json.Jsonize(FIELDS, mFields);
        vector<Any> anyVec;

        int32_t fieldCount = (mBaseFieldCount >= 0) ? mBaseFieldCount : (int32_t)mFields.size();
        anyVec.reserve(fieldCount);
        for (int32_t id = 0; id < fieldCount; id++) {
            FieldConfigPtr field = mFields[id];
            // if (field->IsVirtual()) {
            //     continue;
            // }
            if (ft_enum == field->GetFieldType()) {
                anyVec.push_back(ToJson(*(dynamic_pointer_cast<EnumFieldConfig>(field))));
            } else {
                anyVec.push_back(ToJson(*field));
            }
        }
        json.Jsonize(FIELDS, anyVec);
    }
}

FieldConfigIteratorPtr FieldSchemaImpl::CreateIterator(IndexStatus type) const
{
    vector<FieldConfigPtr> ret;
    for (auto& field : mFields) {
        assert(field);
        if ((field->GetStatus() & type) > 0) {
            ret.push_back(field);
        }
    }
    FieldConfigIteratorPtr iter(new FieldConfigIterator(ret));
    return iter;
}

void FieldSchemaImpl::RewriteFieldType()
{
    for (auto& field : mFields) {
        field->RewriteFieldType();
    }
}
}} // namespace indexlib::config

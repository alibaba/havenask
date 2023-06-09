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
#include "indexlib/config/impl/schema_modify_operation_impl.h"

#include "indexlib/config/attribute_config_creator.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/field_config_loader.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/spatial_index_config.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SchemaModifyOperationImpl);

SchemaModifyOperationImpl::SchemaModifyOperationImpl() : mOpId(INVALID_SCHEMA_OP_ID), mNotReady(false) {}

SchemaModifyOperationImpl::~SchemaModifyOperationImpl() {}

void SchemaModifyOperationImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        JsonMap delMap;
        if (!mDeleteIndexs.empty()) {
            delMap[INDEXS] = autil::legacy::ToJson(mDeleteIndexs);
        }
        if (!mDeleteAttrs.empty()) {
            delMap[ATTRIBUTES] = autil::legacy::ToJson(mDeleteAttrs);
        }
        if (!mDeleteFields.empty()) {
            delMap[FIELDS] = autil::legacy::ToJson(mDeleteFields);
        }
        if (!delMap.empty()) {
            json.Jsonize(SCHEMA_MODIFY_DEL, delMap);
        }

        JsonMap addMap;
        if (!mAddFields.empty()) {
            addMap[FIELDS] = autil::legacy::ToJson(mAddFields);
        }

        vector<string> attrNames;
        for (auto& attr : mAddAttrs) {
            if (attr->GetConfigType() != AttributeConfig::ct_index_accompany) {
                attrNames.push_back(attr->GetAttrName());
            }
        }
        if (!attrNames.empty()) {
            addMap[ATTRIBUTES] = autil::legacy::ToJson(attrNames);
        }

        if (!mAddIndexs.empty()) {
            addMap[INDEXS] = autil::legacy::ToJson(mAddIndexs);
        }
        if (!addMap.empty()) {
            json.Jsonize(SCHEMA_MODIFY_ADD, addMap);
        }

        if (!mParameters.empty()) {
            json.Jsonize(SCHEMA_MODIFY_PARAMETER, mParameters);
        }
    }
}

schema_opid_t SchemaModifyOperationImpl::GetOpId() const { return mOpId; }

void SchemaModifyOperationImpl::SetOpId(schema_opid_t id)
{
    mOpId = id;
    for (auto& item : mAddFields) {
        item->SetOwnerModifyOperationId(id);
    }
    for (auto& item : mAddIndexs) {
        item->SetOwnerModifyOperationId(id);
    }
    for (auto& item : mAddAttrs) {
        item->SetOwnerModifyOperationId(id);
    }
}

void SchemaModifyOperationImpl::LoadDeleteOperation(const Any& any, IndexPartitionSchemaImpl& schema)
{
    JsonWrapper json(any);
    json.Jsonize(INDEXS, mDeleteIndexs, mDeleteIndexs);
    json.Jsonize(ATTRIBUTES, mDeleteAttrs, mDeleteAttrs);
    json.Jsonize(FIELDS, mDeleteFields, mDeleteFields);

    const SummarySchemaPtr& summarySchema = schema.GetSummarySchema();
    const AttributeSchemaPtr& attrSchema = schema.GetAttributeSchema();
    const FieldSchemaPtr& fieldSchema = schema.GetFieldSchema();
    const IndexSchemaPtr& indexSchema = schema.GetIndexSchema();
    set<string> truncateSortFields;
    InitTruncateSortFieldSet(schema, truncateSortFields);

    for (auto& id : mDeleteAttrs) {
        if (!attrSchema) {
            INDEXLIB_FATAL_ERROR(Schema, "fail to delete attribute [%s], no attribute schema", id.c_str());
        }
        if (!fieldSchema) {
            INDEXLIB_FATAL_ERROR(Schema, "delete attribute [%s] fail, no fields!", id.c_str());
        }
        if (truncateSortFields.find(id) != truncateSortFields.end()) {
            INDEXLIB_FATAL_ERROR(Schema,
                                 "delete attribute [%s] fail, "
                                 "which is sort field in truncate_profile!",
                                 id.c_str());
        }
        fieldid_t fieldId = fieldSchema->GetFieldId(id);
        if (fieldId == INVALID_FIELDID) {
            INDEXLIB_FATAL_ERROR(Schema, "delete attribute [%s] fail, it's not in fields!", id.c_str());
        }
        if (summarySchema && summarySchema->IsInSummary(fieldId)) {
            INDEXLIB_FATAL_ERROR(Schema, "delete attribute [%s] fail, shared in summary!", id.c_str());
        }
        attrSchema->DeleteAttribute(id);
    }

    for (auto& id : mDeleteIndexs) {
        if (!indexSchema) {
            INDEXLIB_FATAL_ERROR(Schema, "fail to delete index [%s], no index schema", id.c_str());
        }
        EnsureAccompanyAttributeDeleteForSpatialIndex(id, schema);
        indexSchema->DeleteIndex(id);
    }

    for (auto& id : mDeleteFields) {
        if (!fieldSchema) {
            INDEXLIB_FATAL_ERROR(Schema, "delete field [%s] fail, no fields!", id.c_str());
        }
        fieldid_t fieldId = fieldSchema->GetFieldId(id);
        if (fieldId == INVALID_FIELDID) {
            INDEXLIB_FATAL_ERROR(Schema, "delete field [%s] fail, it's not in fields!", id.c_str());
        }
        if (indexSchema && indexSchema->IsInIndex(fieldId)) {
            INDEXLIB_FATAL_ERROR(Schema, "delete field [%s] fail, still in indexs!", id.c_str());
        }
        if (attrSchema && attrSchema->IsInAttribute(fieldId)) {
            INDEXLIB_FATAL_ERROR(Schema, "delete field [%s] fail, still in attributes!", id.c_str());
        }
        if (summarySchema && summarySchema->IsInSummary(fieldId)) {
            INDEXLIB_FATAL_ERROR(Schema, "delete field [%s] fail, still in summary!", id.c_str());
        }
        fieldSchema->DeleteField(id);
    }
}

void SchemaModifyOperationImpl::LoadAddOperation(const Any& any, IndexPartitionSchemaImpl& schema)
{
    JsonMap jsonMap = AnyCast<JsonMap>(any);
    auto it = jsonMap.find(FIELDS);
    if (it != jsonMap.end()) {
        schema.LoadFieldSchema(it->second, mAddFields);
    }

    it = jsonMap.find(INDEXS);
    if (it != jsonMap.end()) {
        JsonArray indexs = AnyCast<JsonArray>(it->second);
        for (JsonArray::iterator iter = indexs.begin(); iter != indexs.end(); ++iter) {
            IndexConfigPtr indexConfig = IndexConfigCreator::Create(
                schema.GetFieldSchema(), schema.GetDictSchema(), schema.GetAdaptiveDictSchema(),
                schema.GetFileCompressSchema(), *iter, schema.GetTableType(), schema.IsLoadFromIndex());
            if (indexConfig->HasTruncate()) {
                INDEXLIB_FATAL_ERROR(UnSupported, "not support add index [%s] which has truncate",
                                     indexConfig->GetIndexName().c_str());
            }

            if (indexConfig->GetAdaptiveDictionaryConfig()) {
                INDEXLIB_FATAL_ERROR(UnSupported, "not support add index [%s] which use adaptive bitmap",
                                     indexConfig->GetIndexName().c_str());
            }
            schema.AddIndexConfig(indexConfig);
            mAddIndexs.push_back(indexConfig);
        }
    }

    it = jsonMap.find(ATTRIBUTES);
    if (it != jsonMap.end()) {
        JsonArray attrs = AnyCast<JsonArray>(it->second);
        for (JsonArray::iterator iter = attrs.begin(); iter != attrs.end(); ++iter) {
            if (iter->GetType() == typeid(JsonMap)) {
                AttributeConfigPtr attributeConfig =
                    AttributeConfigCreator::Create(schema.GetFieldSchema(), schema.GetFileCompressSchema(), *iter);
                schema.AddAttributeConfig(attributeConfig);
                mAddAttrs.push_back(attributeConfig);
            } else {
                string fieldName = AnyCast<string>(*iter);
                schema.AddAttributeConfig(fieldName);
                mAddAttrs.push_back(schema.GetAttributeSchema()->GetAttributeConfig(fieldName));
            }
        }
    }

    RegionSchemaPtr regionSchema = schema.GetRegionSchema(DEFAULT_REGIONID);
    if (regionSchema) {
        vector<AttributeConfigPtr> indexAccessoryAttrs = regionSchema->EnsureSpatialIndexWithAttribute();
        if (!indexAccessoryAttrs.empty()) {
            mAddAttrs.insert(mAddAttrs.end(), indexAccessoryAttrs.begin(), indexAccessoryAttrs.end());
        }
    }
}

void SchemaModifyOperationImpl::MarkNotReady()
{
    for (auto& index : mAddIndexs) {
        index->Disable();
    }
    for (auto& attr : mAddAttrs) {
        attr->Disable();
    }
    mNotReady = true;
}

void SchemaModifyOperationImpl::CollectEffectiveModifyItem(ModifyItemVector& indexs, ModifyItemVector& attrs)
{
    if (mNotReady) {
        return;
    }

    for (auto& index : mAddIndexs) {
        if (index->IsNormal()) {
            ModifyItemPtr item(new ModifyItem(
                index->GetIndexName(), IndexConfig::InvertedIndexTypeToStr(index->GetInvertedIndexType()), mOpId));
            indexs.push_back(item);
        }
    }

    for (auto& attr : mAddAttrs) {
        if (attr->IsNormal()) {
            ModifyItemPtr item(
                new ModifyItem(attr->GetAttrName(), FieldConfig::FieldTypeToStr(attr->GetFieldType()), mOpId));
            attrs.push_back(item);
        }
    }
}

void SchemaModifyOperationImpl::AssertEqual(const SchemaModifyOperationImpl& other) const
{
    IE_CONFIG_ASSERT_EQUAL(mDeleteFields, other.mDeleteFields, "mDeleteFields not equal");
    IE_CONFIG_ASSERT_EQUAL(mDeleteAttrs, other.mDeleteAttrs, "mDeleteAttrs not equal");
    IE_CONFIG_ASSERT_EQUAL(mDeleteIndexs, other.mDeleteIndexs, "mDeleteIndexs not equal");

    IE_CONFIG_ASSERT_EQUAL(mAddFields.size(), other.mAddFields.size(), "mAddFields size not equal");
    for (size_t i = 0; i < mAddFields.size(); i++) {
        mAddFields[i]->AssertEqual(*(other.mAddFields[i]));
    }

    IE_CONFIG_ASSERT_EQUAL(mAddAttrs.size(), other.mAddAttrs.size(), "mAddAttrs size not equal");
    for (size_t i = 0; i < mAddAttrs.size(); i++) {
        mAddAttrs[i]->AssertEqual(*(other.mAddAttrs[i]));
    }

    IE_CONFIG_ASSERT_EQUAL(mAddIndexs.size(), other.mAddIndexs.size(), "mAddIndexs size not equal");
    for (size_t i = 0; i < mAddIndexs.size(); i++) {
        mAddIndexs[i]->AssertEqual(*(other.mAddIndexs[i]));
    }

    IE_CONFIG_ASSERT_EQUAL(mParameters, other.mParameters, "mParameters not equal");
}

void SchemaModifyOperationImpl::EnsureAccompanyAttributeDeleteForSpatialIndex(const string& indexName,
                                                                              IndexPartitionSchemaImpl& schema)
{
    const SummarySchemaPtr& summarySchema = schema.GetSummarySchema();
    const AttributeSchemaPtr& attrSchema = schema.GetAttributeSchema();
    const FieldSchemaPtr& fieldSchema = schema.GetFieldSchema();
    const IndexSchemaPtr& indexSchema = schema.GetIndexSchema();
    assert(indexSchema);
    if (!attrSchema || !fieldSchema) {
        return;
    }

    IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    if (!indexConfig || indexConfig->GetInvertedIndexType() != it_spatial) {
        return;
    }

    SpatialIndexConfigPtr spatialIndexConf = DYNAMIC_POINTER_CAST(SpatialIndexConfig, indexConfig);
    assert(spatialIndexConf);
    FieldConfigPtr fieldConfig = spatialIndexConf->GetFieldConfig();
    assert(fieldConfig);
    if (fieldConfig->GetFieldType() != ft_location) {
        return;
    }

    string fieldName = fieldConfig->GetFieldName();
    AttributeConfigPtr attrConfig = attrSchema->GetAttributeConfig(fieldName);
    if (!attrConfig || attrConfig->GetConfigType() != AttributeConfig::ct_index_accompany) {
        return;
    }
    fieldid_t fieldId = fieldSchema->GetFieldId(fieldName);
    if (fieldId == INVALID_FIELDID) {
        INDEXLIB_FATAL_ERROR(Schema, "delete attribute [%s] fail, it's not in fields!", fieldName.c_str());
    }
    if (summarySchema && summarySchema->IsInSummary(fieldId)) {
        return;
    }
    attrSchema->DeleteAttribute(fieldName);
}

void SchemaModifyOperationImpl::Validate() const
{
    if (!mDeleteFields.empty()) {
        return;
    }
    if (!mDeleteIndexs.empty()) {
        return;
    }
    if (!mDeleteAttrs.empty()) {
        return;
    }
    if (!mAddFields.empty()) {
        return;
    }
    if (!mAddIndexs.empty()) {
        return;
    }
    if (!mAddAttrs.empty()) {
        return;
    }
    INDEXLIB_FATAL_ERROR(Schema, "empty schema modify operation!");
}

void SchemaModifyOperationImpl::InitTruncateSortFieldSet(const IndexPartitionSchemaImpl& schema,
                                                         set<string>& truncateSortFields)
{
    TruncateProfileSchemaPtr truncProfileSchema = schema.GetTruncateProfileSchema(DEFAULT_REGIONID);
    if (!truncProfileSchema) {
        return;
    }

    for (auto iter = truncProfileSchema->Begin(); iter != truncProfileSchema->End(); iter++) {
        const SortParams& sortParams = iter->second->GetTruncateSortParams();
        for (auto& param : sortParams) {
            truncateSortFields.insert(param.GetSortField());
        }
    }
}
}} // namespace indexlib::config

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
#include "build_service/processor/LegacyModifiedFieldsModifierCreator.h"

#include <assert.h>
#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "build_service/processor/ModifiedFieldsIgnoreModifier.h"
#include "build_service/processor/ModifiedFieldsSubRelationModifier.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/document/normal/ClassifiedDocument.h"

using namespace std;
using namespace indexlib::config;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, LegacyModifiedFieldsModifierCreator);

LegacyModifiedFieldsModifierCreator::LegacyModifiedFieldsModifierCreator() {}

LegacyModifiedFieldsModifierCreator::~LegacyModifiedFieldsModifierCreator() {}

fieldid_t LegacyModifiedFieldsModifierCreator::getFieldId(const string& fieldName,
                                                          const IndexPartitionSchemaPtr& schema,
                                                          ModifiedFieldsModifier::SchemaType& schemaType)
{
    fieldid_t srcFid = schema->GetFieldId(fieldName);
    if (srcFid != INVALID_FIELDID) {
        schemaType = ModifiedFieldsModifier::ST_MAIN;
        return srcFid;
    }

    const IndexPartitionSchemaPtr& subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        srcFid = subSchema->GetFieldId(fieldName);
        if (srcFid != INVALID_FIELDID) {
            schemaType = ModifiedFieldsModifier::ST_SUB;
            return srcFid;
        }
    }

    schemaType = ModifiedFieldsModifier::ST_UNKNOWN;
    return INVALID_FIELDID;
}

fieldid_t LegacyModifiedFieldsModifierCreator::getUnknowFieldId(const string& fieldName, FieldIdMap& unknownFieldIdMap)
{
    FieldIdMap::const_iterator it = unknownFieldIdMap.find(fieldName);
    if (it != unknownFieldIdMap.end()) {
        return it->second;
    }
    fieldid_t fieldId = (fieldid_t)unknownFieldIdMap.size();
    unknownFieldIdMap.insert(make_pair(fieldName, fieldId));
    return fieldId;
}

ModifiedFieldsModifierPtr LegacyModifiedFieldsModifierCreator::createModifiedFieldsModifier(
    const string& srcFieldName, const string& dstFieldName, const indexlib::config::IndexPartitionSchemaPtr& schema,
    FieldIdMap& unknownFieldIdMap)
{
    ModifiedFieldsModifier::SchemaType srcType;
    fieldid_t srcFid = getFieldId(srcFieldName, schema, srcType);
    if (srcFid == INVALID_FIELDID) {
        srcFid = getUnknowFieldId(srcFieldName, unknownFieldIdMap);
        srcType = ModifiedFieldsModifier::ST_UNKNOWN;
    }

    ModifiedFieldsModifier::SchemaType dstType;
    fieldid_t dstFid = getFieldId(dstFieldName, schema, dstType);
    if (dstFid == INVALID_FIELDID) {
        string errorMsg = "failed to get field id [" + dstFieldName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return ModifiedFieldsModifierPtr();
    }

    if (schema->GetSubIndexPartitionSchema()) {
        return ModifiedFieldsModifierPtr(new ModifiedFieldsSubRelationModifier(srcFid, srcType, dstFid, dstType));
    }

    return ModifiedFieldsModifierPtr(new ModifiedFieldsModifier(srcFid, srcType, dstFid, dstType));
}

ModifiedFieldsModifierPtr
LegacyModifiedFieldsModifierCreator::createIgnoreModifier(const std::string& ignoreFieldName,
                                                          const indexlib::config::IndexPartitionSchemaPtr& schema)
{
    assert(!ignoreFieldName.empty());

    string pkNameInMainSchema = schema->GetIndexSchema()->GetPrimaryKeyIndexFieldName();
    if (ignoreFieldName == pkNameInMainSchema) {
        string errorMsg = "can not ignore pk [" + pkNameInMainSchema + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return ModifiedFieldsModifierPtr();
    }

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        const IndexSchemaPtr& indexSchema = subSchema->GetIndexSchema();
        if (indexSchema->HasPrimaryKeyIndex()) {
            string pkNameInSubSchema = indexSchema->GetPrimaryKeyIndexFieldName();
            if (ignoreFieldName == pkNameInSubSchema) {
                string errorMsg = "can not ignore pk [" + ignoreFieldName + "] in sub";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return ModifiedFieldsModifierPtr();
            }
        }
    }

    ModifiedFieldsModifier::SchemaType ignoreType;
    fieldid_t ignoreFid = getFieldId(ignoreFieldName, schema, ignoreType);

    if (ignoreFid == INVALID_FIELDID) {
        string errorMsg = "failed to get field id [" + ignoreFieldName + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return ModifiedFieldsModifierPtr();
    }

    return ModifiedFieldsModifierPtr(new ModifiedFieldsIgnoreModifier(ignoreFid, ignoreType));
}

}} // namespace build_service::processor

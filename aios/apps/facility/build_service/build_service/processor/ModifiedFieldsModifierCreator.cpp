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
#include "build_service/processor/ModifiedFieldsModifierCreator.h"

#include <assert.h>
#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "build_service/processor/LegacyModifiedFieldsModifierCreator.h"
#include "build_service/processor/ModifiedFieldsIgnoreModifier.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/document/normal/ClassifiedDocument.h"

using namespace std;
using namespace indexlib::config;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsModifierCreator);

ModifiedFieldsModifierCreator::ModifiedFieldsModifierCreator() {}

ModifiedFieldsModifierCreator::~ModifiedFieldsModifierCreator() {}

fieldid_t ModifiedFieldsModifierCreator::getFieldId(const string& fieldName,
                                                    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                                                    ModifiedFieldsModifier::SchemaType& schemaType)
{
    fieldid_t srcFid = schema->GetFieldId(fieldName);
    if (srcFid != INVALID_FIELDID) {
        schemaType = ModifiedFieldsModifier::ST_MAIN;
        return srcFid;
    }
    schemaType = ModifiedFieldsModifier::ST_UNKNOWN;
    return INVALID_FIELDID;
}

fieldid_t ModifiedFieldsModifierCreator::getUnknowFieldId(const string& fieldName, FieldIdMap& unknownFieldIdMap)
{
    FieldIdMap::const_iterator it = unknownFieldIdMap.find(fieldName);
    if (it != unknownFieldIdMap.end()) {
        return it->second;
    }
    fieldid_t fieldId = (fieldid_t)unknownFieldIdMap.size();
    unknownFieldIdMap.insert(make_pair(fieldName, fieldId));
    return fieldId;
}

ModifiedFieldsModifierPtr ModifiedFieldsModifierCreator::createModifiedFieldsModifier(
    const string& srcFieldName, const string& dstFieldName,
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema, FieldIdMap& unknownFieldIdMap)
{
    auto legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
    if (legacySchemaAdapter) {
        return LegacyModifiedFieldsModifierCreator::createModifiedFieldsModifier(
            srcFieldName, dstFieldName, legacySchemaAdapter->GetLegacySchema(), unknownFieldIdMap);
    }

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
    return ModifiedFieldsModifierPtr(new ModifiedFieldsModifier(srcFid, srcType, dstFid, dstType));
}

ModifiedFieldsModifierPtr
ModifiedFieldsModifierCreator::createIgnoreModifier(const std::string& ignoreFieldName,
                                                    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema)
{
    assert(!ignoreFieldName.empty());
    auto legacySchemaAdapter = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
    if (legacySchemaAdapter) {
        return LegacyModifiedFieldsModifierCreator::createIgnoreModifier(ignoreFieldName,
                                                                         legacySchemaAdapter->GetLegacySchema());
    }

    auto pkConfig = schema->GetPrimaryKeyIndexConfig();
    if (pkConfig) {
        auto pkIndexName = pkConfig->GetIndexName();
        if (ignoreFieldName == pkIndexName) {
            string errorMsg = "can not ignore pk [" + pkIndexName + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return ModifiedFieldsModifierPtr();
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

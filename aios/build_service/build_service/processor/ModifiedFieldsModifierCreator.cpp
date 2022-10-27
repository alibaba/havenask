#include <indexlib/config/index_partition_schema.h>
#include "build_service/processor/ModifiedFieldsModifierCreator.h"
#include "build_service/processor/ModifiedFieldsSubRelationModifier.h"
#include "build_service/processor/ModifiedFieldsIgnoreModifier.h"

using namespace std;
IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsModifierCreator);

ModifiedFieldsModifierCreator::ModifiedFieldsModifierCreator() {
}

ModifiedFieldsModifierCreator::~ModifiedFieldsModifierCreator() {
}

fieldid_t ModifiedFieldsModifierCreator::getFieldId(const string &fieldName,
        const IndexPartitionSchemaPtr &schema,
        ModifiedFieldsModifier::SchemaType &schemaType)
{
    const FieldSchemaPtr &fieldSchema = schema->GetFieldSchema();
    assert(fieldSchema);
    
    fieldid_t srcFid = fieldSchema->GetFieldId(fieldName);
    if (srcFid != INVALID_FIELDID) {
        schemaType = ModifiedFieldsModifier::ST_MAIN;
        return srcFid;
    }

    const IndexPartitionSchemaPtr &subSchema =
        schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        const FieldSchemaPtr &subFieldSchema = subSchema->GetFieldSchema();
        assert(subFieldSchema);
        srcFid = subFieldSchema->GetFieldId(fieldName);
        if (srcFid != INVALID_FIELDID) {
            schemaType = ModifiedFieldsModifier::ST_SUB;
            return srcFid;
        }
    }

    schemaType = ModifiedFieldsModifier::ST_UNKNOWN;
    return INVALID_FIELDID;
}

fieldid_t ModifiedFieldsModifierCreator::getUnknowFieldId(const string &fieldName,
        FieldIdMap &unknownFieldIdMap)
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
        const string &srcFieldName, const string &dstFieldName,
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
        FieldIdMap &unknownFieldIdMap)
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
        return ModifiedFieldsModifierPtr(new ModifiedFieldsSubRelationModifier(
                        srcFid, srcType, dstFid, dstType));
    }
    
    return ModifiedFieldsModifierPtr(new ModifiedFieldsModifier(srcFid, srcType, dstFid, dstType));
}

ModifiedFieldsModifierPtr ModifiedFieldsModifierCreator::createIgnoreModifier(
        const std::string &ignoreFieldName,
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema)
{
    assert(!ignoreFieldName.empty());

    string pkNameInMainSchema =
        schema->GetIndexSchema()->GetPrimaryKeyIndexFieldName();
    if (ignoreFieldName == pkNameInMainSchema) {
        string errorMsg = "can not ignore pk [" + pkNameInMainSchema + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return ModifiedFieldsModifierPtr();
    }

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        const IndexSchemaPtr &indexSchema = subSchema->GetIndexSchema();
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
    
}
}

#ifndef ISEARCH_BS_MODIFIEDFIELDSMODIFIERCREATOR_H
#define ISEARCH_BS_MODIFIEDFIELDSMODIFIERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/ModifiedFieldsModifier.h"

IE_NAMESPACE_BEGIN(config);
class IndexPartitionSchema;
typedef std::tr1::shared_ptr<IndexPartitionSchema> IndexPartitionSchemaPtr;
IE_NAMESPACE_END(config);

namespace build_service {
namespace processor {

class ModifiedFieldsModifierCreator
{
public:
    typedef std::map<std::string, fieldid_t> FieldIdMap;

public:
    ModifiedFieldsModifierCreator();
    ~ModifiedFieldsModifierCreator();

public:
    static ModifiedFieldsModifierPtr createModifiedFieldsModifier(
            const std::string& srcFieldName, const std::string& dstFieldName,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
            FieldIdMap& unknownFieldIdMap);

    static ModifiedFieldsModifierPtr createIgnoreModifier(
            const std::string& ignoreFieldName,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema);

private:
    static fieldid_t getFieldId(
            const std::string &fieldName,
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema,
            ModifiedFieldsModifier::SchemaType &schemaType);
    static fieldid_t getUnknowFieldId(const std::string &fieldName,
            FieldIdMap &unknownFieldIdMap);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsModifierCreator);

}
}

#endif //ISEARCH_BS_MODIFIEDFIELDSMODIFIERCREATOR_H

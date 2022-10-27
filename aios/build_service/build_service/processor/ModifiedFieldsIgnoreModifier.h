#ifndef ISEARCH_BS_MODIFIEDFIELDSIGNOREMODIFIER_H
#define ISEARCH_BS_MODIFIEDFIELDSIGNOREMODIFIER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/ExtendDocument.h"
#include "build_service/processor/ModifiedFieldsModifier.h"

namespace build_service {
namespace processor {

class ModifiedFieldsIgnoreModifier : public ModifiedFieldsModifier
{
public:
    ModifiedFieldsIgnoreModifier(fieldid_t ignoreFieldId, SchemaType ignoreFieldType);
    ~ModifiedFieldsIgnoreModifier();

public:
    bool process(const document::ExtendDocumentPtr &document,
                 FieldIdSet &unknownFieldIdSet);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsIgnoreModifier);

}
}

#endif //ISEARCH_BS_MODIFIEDFIELDSIGNOREMODIFIER_H

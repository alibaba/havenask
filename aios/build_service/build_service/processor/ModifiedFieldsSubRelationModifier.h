#ifndef ISEARCH_BS_MODIFIEDFIELDSSUBRELATIONMODIFIER_H
#define ISEARCH_BS_MODIFIEDFIELDSSUBRELATIONMODIFIER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/ModifiedFieldsModifier.h"

namespace build_service {
namespace processor {

class ModifiedFieldsSubRelationModifier : public ModifiedFieldsModifier
{
public:
    ModifiedFieldsSubRelationModifier(fieldid_t src, SchemaType srcType,
            fieldid_t dst, SchemaType dstType);
    ~ModifiedFieldsSubRelationModifier();

public:
    virtual bool process(const document::ExtendDocumentPtr &document,
                         FieldIdSet &unknownFieldIdSet);

private:
    typedef document::ExtendDocument::ExtendDocumentVec ExtendDocumentVec;
    bool matchSubDocs(const document::ExtendDocumentPtr &document,
                      ExtendDocumentVec &matchedSubDocs);

    void addToAllDocs(const ExtendDocumentVec &docs);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsSubRelationModifier);

}
}

#endif //ISEARCH_BS_MODIFIEDFIELDSSUBRELATIONMODIFIER_H

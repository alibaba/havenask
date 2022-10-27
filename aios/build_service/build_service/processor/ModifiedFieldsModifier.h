#ifndef ISEARCH_BS_MODIFIEDFIELDSMODIFIER_H
#define ISEARCH_BS_MODIFIEDFIELDSMODIFIER_H

#include <indexlib/document/extend_document/indexlib_extend_document.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/ExtendDocument.h"

namespace build_service {
namespace processor {

class ModifiedFieldsModifier
{
public:
    enum SchemaType {
        ST_MAIN = 0,
        ST_SUB,
        ST_UNKNOWN,
    };
    typedef IE_NAMESPACE(document)::IndexlibExtendDocument::FieldIdSet FieldIdSet;

public:
    ModifiedFieldsModifier(fieldid_t src, SchemaType srcType,
                           fieldid_t dst, SchemaType dstType);
    virtual ~ModifiedFieldsModifier();

public:
    virtual bool process(const document::ExtendDocumentPtr &document,
                         FieldIdSet &unknownFieldIdSet);

public:
    fieldid_t GetSrcFieldId() const { return _src; }
    fieldid_t GetDstFieldId() const { return _dst; }

protected:
    bool match(const document::ExtendDocumentPtr &document,
               FieldIdSet &unknownFieldIdSet) const;

protected:
    fieldid_t _src;
    SchemaType _srcType;
    fieldid_t _dst;
    SchemaType _dstType;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ModifiedFieldsModifier);

}
}


#endif //ISEARCH_BS_MODIFIEDFIELDSMODIFIER_H

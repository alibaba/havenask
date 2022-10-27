#include "build_service/processor/ModifiedFieldsModifier.h"

using namespace std;
using namespace build_service::document;
IE_NAMESPACE_USE(document);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsModifier);

ModifiedFieldsModifier::ModifiedFieldsModifier(fieldid_t src, SchemaType srcType,
        fieldid_t dst, SchemaType dstType)
    : _src(src)
    , _srcType(srcType)
    , _dst(dst)
    , _dstType(dstType)
{
}

ModifiedFieldsModifier::~ModifiedFieldsModifier() {
}

bool ModifiedFieldsModifier::process(const ExtendDocumentPtr &document,
                                     FieldIdSet &unknownFieldIdSet)
{
    if (match(document, unknownFieldIdSet)) {
        const IndexlibExtendDocumentPtr& extDoc = document->getIndexExtendDoc();
        extDoc->addModifiedField(_dst);
    }

    return true;
}

bool ModifiedFieldsModifier::match(const ExtendDocumentPtr &document,
                                   FieldIdSet &unknownFieldIdSet) const
{
    if (_srcType == ST_MAIN) {
        const IndexlibExtendDocumentPtr& extDoc = document->getIndexExtendDoc();        
        if (extDoc->hasFieldInModifiedFieldSet(_src)) {
            return true;
        }
    } else if (_srcType == ST_UNKNOWN) {
        if (unknownFieldIdSet.count(_src) > 0) {
            return true;
        }
    }
    return false;
}

}
}

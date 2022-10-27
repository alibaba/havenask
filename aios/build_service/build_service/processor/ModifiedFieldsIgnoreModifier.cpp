#include "build_service/processor/ModifiedFieldsIgnoreModifier.h"

using namespace std;
using namespace build_service::document;
IE_NAMESPACE_USE(document);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsIgnoreModifier);

ModifiedFieldsIgnoreModifier::ModifiedFieldsIgnoreModifier(
        fieldid_t ignoreFieldId, SchemaType ignoreFieldType)
    : ModifiedFieldsModifier(ignoreFieldId, ignoreFieldType,
                             ignoreFieldId, ignoreFieldType)
{
}

ModifiedFieldsIgnoreModifier::~ModifiedFieldsIgnoreModifier() {
}

bool ModifiedFieldsIgnoreModifier::process(const ExtendDocumentPtr &document,
        FieldIdSet &unknownFieldIdSet)
{
    const IndexlibExtendDocumentPtr& extDoc = document->getIndexExtendDoc();
    if (_dstType == ST_MAIN) {
        extDoc->removeModifiedField(_dst);
    } else if (_dstType == ST_SUB) {
        extDoc->removeSubModifiedField(_dst);
        for (size_t i = 0; i < document->getSubDocumentsCount(); ++i) {
            ExtendDocumentPtr &subDoc = document->getSubDocument(i);
            const IndexlibExtendDocumentPtr& subExtDoc = subDoc->getIndexExtendDoc();
            subExtDoc->removeModifiedField(_dst);
        }
    }
    return true;
}

}
}

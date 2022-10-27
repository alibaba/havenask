#include "build_service/processor/ModifiedFieldsSubRelationModifier.h"

using namespace std;
using namespace build_service::document;

IE_NAMESPACE_USE(document);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsSubRelationModifier);

ModifiedFieldsSubRelationModifier::ModifiedFieldsSubRelationModifier(
        fieldid_t src, SchemaType srcType,
        fieldid_t dst, SchemaType dstType)
    : ModifiedFieldsModifier(src, srcType, dst, dstType)
{
}

ModifiedFieldsSubRelationModifier::~ModifiedFieldsSubRelationModifier() {
}

void ModifiedFieldsSubRelationModifier::addToAllDocs(const ExtendDocumentVec &docs)
{
    for (size_t i = 0; i < docs.size(); ++i) {
        const ExtendDocumentPtr &doc = docs[i];
        const IndexlibExtendDocumentPtr &extDoc = doc->getIndexExtendDoc();
        extDoc->addModifiedField(_dst);
    }
}

bool ModifiedFieldsSubRelationModifier::matchSubDocs(
        const ExtendDocumentPtr &document, ExtendDocumentVec &matchedSubDocs)
{
    bool ret = false;
    for (size_t i = 0; i < document->getSubDocumentsCount(); ++i) {
        ExtendDocumentPtr& subDoc = document->getSubDocument(i);
        const IndexlibExtendDocumentPtr& subExtDoc = subDoc->getIndexExtendDoc();
        if (subExtDoc->hasFieldInModifiedFieldSet(_src)) {
            matchedSubDocs.push_back(subDoc);
            ret = true;
        }
    }
    return ret;
}

bool ModifiedFieldsSubRelationModifier::process(const ExtendDocumentPtr &document,
        FieldIdSet &unknownFieldIdSet)
{
    assert(_dstType != ST_UNKNOWN);

    if (_dstType == ST_MAIN && (_srcType == ST_MAIN || _srcType == ST_UNKNOWN)) {
        return ModifiedFieldsModifier::process(document, unknownFieldIdSet);
    }

    if (_dstType == ST_SUB && (_srcType == ST_MAIN || _srcType == ST_UNKNOWN)) {
        if (match(document, unknownFieldIdSet)) {
            addToAllDocs(document->getAllSubDocuments());
            const IndexlibExtendDocumentPtr &extDoc = document->getIndexExtendDoc();
            extDoc->addSubModifiedField(_dst);
        }
        return true;
    }

    if (_dstType == ST_MAIN && _srcType == ST_SUB) {
        ExtendDocumentVec matchedSubDocs;
        if (matchSubDocs(document, matchedSubDocs)) {
            const IndexlibExtendDocumentPtr &extDoc = document->getIndexExtendDoc();
            extDoc->addModifiedField(_dst);
        }
        return true;
    }

    if (_dstType == ST_SUB && _srcType == ST_SUB) {
        ExtendDocumentVec matchedSubDocs;
        if (matchSubDocs(document, matchedSubDocs)) {
            addToAllDocs(matchedSubDocs);
            const IndexlibExtendDocumentPtr &extDoc = document->getIndexExtendDoc();
            extDoc->addSubModifiedField(_dst);
        }
        return true;
    }
    
    assert(false);
    return false;
}

}
}

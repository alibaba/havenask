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
#include "build_service/processor/ModifiedFieldsSubRelationModifier.h"

using namespace std;
using namespace build_service::document;

using namespace indexlib::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsSubRelationModifier);

ModifiedFieldsSubRelationModifier::ModifiedFieldsSubRelationModifier(fieldid_t src, SchemaType srcType, fieldid_t dst,
                                                                     SchemaType dstType)
    : ModifiedFieldsModifier(src, srcType, dst, dstType)
{
}

ModifiedFieldsSubRelationModifier::~ModifiedFieldsSubRelationModifier() {}

void ModifiedFieldsSubRelationModifier::addToAllDocs(const ExtendDocumentVec& docs)
{
    for (size_t i = 0; i < docs.size(); ++i) {
        const ExtendDocumentPtr& doc = docs[i];
        doc->addModifiedField(_dst);
    }
}

bool ModifiedFieldsSubRelationModifier::matchSubDocs(const ExtendDocumentPtr& document,
                                                     ExtendDocumentVec& matchedSubDocs)
{
    bool ret = false;
    for (size_t i = 0; i < document->getSubDocumentsCount(); ++i) {
        ExtendDocumentPtr& subDoc = document->getSubDocument(i);
        if (subDoc->hasFieldInModifiedFieldSet(_src)) {
            matchedSubDocs.push_back(subDoc);
            ret = true;
        }
    }
    return ret;
}

bool ModifiedFieldsSubRelationModifier::process(const ExtendDocumentPtr& document, FieldIdSet& unknownFieldIdSet)
{
    assert(_dstType != ST_UNKNOWN);

    if (_dstType == ST_MAIN && (_srcType == ST_MAIN || _srcType == ST_UNKNOWN)) {
        return ModifiedFieldsModifier::process(document, unknownFieldIdSet);
    }

    if (_dstType == ST_SUB && (_srcType == ST_MAIN || _srcType == ST_UNKNOWN)) {
        if (match(document, unknownFieldIdSet)) {
            addToAllDocs(document->getAllSubDocuments());
            const IndexlibExtendDocumentPtr& extDoc = document->getLegacyExtendDoc();
            assert(extDoc);
            extDoc->addSubModifiedField(_dst);
        }
        return true;
    }

    if (_dstType == ST_MAIN && _srcType == ST_SUB) {
        ExtendDocumentVec matchedSubDocs;
        if (matchSubDocs(document, matchedSubDocs)) {
            document->addModifiedField(_dst);
        }
        return true;
    }

    if (_dstType == ST_SUB && _srcType == ST_SUB) {
        ExtendDocumentVec matchedSubDocs;
        if (matchSubDocs(document, matchedSubDocs)) {
            addToAllDocs(matchedSubDocs);
            const IndexlibExtendDocumentPtr& extDoc = document->getLegacyExtendDoc();
            assert(extDoc);
            extDoc->addSubModifiedField(_dst);
        }
        return true;
    }

    assert(false);
    return false;
}

}} // namespace build_service::processor

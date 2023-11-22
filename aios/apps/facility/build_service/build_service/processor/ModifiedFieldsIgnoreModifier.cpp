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
#include "build_service/processor/ModifiedFieldsIgnoreModifier.h"

#include <assert.h>
#include <cstddef>
#include <memory>

#include "build_service/document/ClassifiedDocument.h"
#include "indexlib/document/extend_document/indexlib_extend_document.h"
#include "indexlib/document/normal/Field.h"

using namespace std;
using namespace build_service::document;
using namespace indexlib::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsIgnoreModifier);

ModifiedFieldsIgnoreModifier::ModifiedFieldsIgnoreModifier(fieldid_t ignoreFieldId, SchemaType ignoreFieldType)
    : ModifiedFieldsModifier(ignoreFieldId, ignoreFieldType, ignoreFieldId, ignoreFieldType)
{
}

ModifiedFieldsIgnoreModifier::~ModifiedFieldsIgnoreModifier() {}

bool ModifiedFieldsIgnoreModifier::process(const ExtendDocumentPtr& document, FieldIdSet& unknownFieldIdSet)
{
    if (_dstType == ST_MAIN) {
        document->removeModifiedField(_dst);
    } else if (_dstType == ST_SUB) {
        const IndexlibExtendDocumentPtr& extDoc = document->getLegacyExtendDoc();
        assert(extDoc);
        extDoc->removeSubModifiedField(_dst);
        for (size_t i = 0; i < document->getSubDocumentsCount(); ++i) {
            ExtendDocumentPtr& subDoc = document->getSubDocument(i);
            subDoc->removeModifiedField(_dst);
        }
    }
    return true;
}

}} // namespace build_service::processor

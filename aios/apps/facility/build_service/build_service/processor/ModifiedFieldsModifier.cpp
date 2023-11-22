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
#include "build_service/processor/ModifiedFieldsModifier.h"

#include <iosfwd>
#include <memory>

#include "build_service/document/ClassifiedDocument.h"
#include "indexlib/document/normal/Field.h"

using namespace std;
using namespace build_service::document;
using namespace indexlib::document;

namespace build_service { namespace processor {
BS_LOG_SETUP(processor, ModifiedFieldsModifier);

ModifiedFieldsModifier::ModifiedFieldsModifier(fieldid_t src, SchemaType srcType, fieldid_t dst, SchemaType dstType)
    : _src(src)
    , _srcType(srcType)
    , _dst(dst)
    , _dstType(dstType)
{
}

ModifiedFieldsModifier::~ModifiedFieldsModifier() {}

bool ModifiedFieldsModifier::process(const ExtendDocumentPtr& document, FieldIdSet& unknownFieldIdSet)
{
    if (match(document, unknownFieldIdSet)) {
        document->addModifiedField(_dst);
    }

    return true;
}

bool ModifiedFieldsModifier::match(const ExtendDocumentPtr& document, FieldIdSet& unknownFieldIdSet) const
{
    if (_srcType == ST_MAIN) {
        if (document->hasFieldInModifiedFieldSet(_src)) {
            return true;
        }
    } else if (_srcType == ST_UNKNOWN) {
        if (unknownFieldIdSet.count(_src) > 0) {
            return true;
        }
    }
    return false;
}

}} // namespace build_service::processor

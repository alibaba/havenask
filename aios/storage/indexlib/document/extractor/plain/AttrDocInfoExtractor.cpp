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
#include "indexlib/document/extractor/plain/AttrDocInfoExtractor.h"

#include <memory>

#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/NormalDocument.h"

using namespace indexlibv2::document;
using namespace indexlibv2::document::extractor;

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.document, AttrDocInfoExtractor);

Status AttrDocInfoExtractor::ExtractField(IDocument* doc, void** field)
{
    indexlibv2::document::NormalDocument* normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        AUTIL_LOG(ERROR, "can not extract attribute information from the given document.");
        return Status::Unknown("can not extract attribute information from the given document.");
    }
    const auto& attrDoc = normalDoc->GetAttributeDocument();
    *field = (void*)attrDoc.get();
    return Status::OK();
}

bool AttrDocInfoExtractor::IsValidDocument(IDocument* doc)
{
    indexlibv2::document::NormalDocument* normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        return false;
    }
    if (doc->GetDocOperateType() != ADD_DOC and doc->GetDocOperateType() != UPDATE_FIELD) {
        return true;
    }
    const auto& attrDoc = normalDoc->GetAttributeDocument();
    if (!attrDoc) {
        return false;
    }
    return true;
}

} // namespace indexlibv2::plain

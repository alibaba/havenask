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
#include "indexlib/document/extractor/plain/FieldMetaFieldInfoExtractor.h"

#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/NormalDocument.h"

namespace indexlib::plain {
AUTIL_LOG_SETUP(indexlib.document, FieldMetaFieldInfoExtractor);

Status FieldMetaFieldInfoExtractor::ExtractField(indexlibv2::document::IDocument* doc, void** field)
{
    indexlibv2::document::NormalDocument* normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        return Status::Corruption("can not extract field meta from the given document.");
    }
    const auto& fieldMetaDoc = normalDoc->GetFieldMetaDocument();
    if (!fieldMetaDoc) {
        return Status::Corruption("can not extract field meta from the given document.");
    }
    bool isNull = false;
    auto* fieldInfo = (std::pair<autil::StringView, bool>*)(*field);
    if (!fieldInfo) {
        AUTIL_LOG(ERROR, "can not extract field meta information from fieldId [%d].", _fieldId);
        return Status::Corruption("can not extract field meta information from fieldId [%d].", _fieldId);
    }
    fieldInfo->first = fieldMetaDoc->GetField(_fieldId, isNull);
    fieldInfo->second = isNull;
    return Status::OK();
}

bool FieldMetaFieldInfoExtractor::IsValidDocument(indexlibv2::document::IDocument* doc)
{
    indexlibv2::document::NormalDocument* normalDoc = dynamic_cast<indexlibv2::document::NormalDocument*>(doc);
    if (!normalDoc) {
        return false;
    }
    if (doc->GetDocOperateType() != ADD_DOC) {
        return true;
    }
    const auto& fieldMetaDoc = normalDoc->GetFieldMetaDocument();
    if (!fieldMetaDoc) {
        return false;
    }
    return true;
}

} // namespace indexlib::plain
